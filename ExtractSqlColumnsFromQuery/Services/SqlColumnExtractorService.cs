using Microsoft.SqlServer.TransactSql.ScriptDom;
using System.Text.RegularExpressions;

namespace ExtractSqlColumnsFromQuery.Services;

public sealed class SqlColumnExtractorService : ISqlColumnExtractorService
{
    private static readonly Regex QualifiedColumnRegex = new(
        @"\b[A-Za-z_][A-Za-z0-9_]*\s*\.\s*(?:\[(?<c>[^\]]+)\]|\""(?<c>[^\""\r\n]+)\""|(?<c>[A-Za-z_][A-Za-z0-9_]*))",
        RegexOptions.IgnoreCase | RegexOptions.Compiled,
        TimeSpan.FromSeconds(1)
    );

    private static readonly Regex AliasRegex = new(
        @"\bAS\s+(?:\[(?<a>[^\]]+)\]|\""(?<a>[^\""\r\n]+)\""|(?<a>[A-Za-z_][A-Za-z0-9_]*))",
        RegexOptions.IgnoreCase | RegexOptions.Compiled,
        TimeSpan.FromSeconds(1)
    );

    public IEnumerable<string> ExtractColumns(string sqlQuery)
    {
        if (string.IsNullOrWhiteSpace(sqlQuery))
        {
            return Enumerable.Empty<string>();
        }

        try
        {
            var parser = new TSql170Parser(initialQuotedIdentifiers: false);
            var script = parser.Parse(new StringReader(sqlQuery), out var errors);
            var columns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var visitor = new ColumnExtractorVisitor();
            script.Accept(visitor);

            foreach (var column in visitor.Columns)
            {
                columns.Add(column);
            }

            // If T-SQL parse fails heavily (e.g. non T-SQL dialect fragments),
            // fallback to conservative regex extraction so the endpoint still returns useful results.
            if (columns.Count == 0 || errors.Count > 0)
            {
                foreach (var fallbackColumn in ExtractColumnsByRegex(sqlQuery))
                {
                    columns.Add(fallbackColumn);
                }
            }

            return columns.OrderBy(c => c).ToList();
        }
        catch (Exception)
        {
            // If parsing fails completely, fallback to regex-based extraction.
            return ExtractColumnsByRegex(sqlQuery);
        }
    }

    private static IEnumerable<string> ExtractColumnsByRegex(string sqlQuery)
    {
        var results = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        // Qualified references: alias.ColumnName or table.ColumnName
        foreach (Match match in QualifiedColumnRegex.Matches(sqlQuery))
        {
            var columnName = match.Groups["c"].Value;
            if (!ColumnExtractorVisitor.IsSystemOrReserved(columnName))
            {
                results.Add(columnName);
            }
        }

        // Aliases: ... AS AliasName
        foreach (Match match in AliasRegex.Matches(sqlQuery))
        {
            var alias = match.Groups["a"].Value;
            if (!ColumnExtractorVisitor.IsSystemOrReserved(alias))
            {
                results.Add(alias);
            }
        }

        return results.OrderBy(x => x).ToList();
    }

    /// <summary>
    /// AST visitor that walks the parse tree and extracts column references.
    /// </summary>
    private sealed class ColumnExtractorVisitor : TSqlFragmentVisitor
    {
        private static readonly HashSet<string> Reserved =
        [
            "AS", "AND", "OR", "NOT", "IN", "EXISTS", "BETWEEN", "LIKE", "IS", "NULL",
            "TRUE", "FALSE", "GETDATE", "SYSDATETIME", "CURRENT_TIMESTAMP",
            "YEAR", "MONTH", "DAY", "DATEADD", "DATEDIFF",
            "COUNT", "SUM", "AVG", "MIN", "MAX", "CAST", "CONVERT",
            "CASE", "WHEN", "THEN", "ELSE", "END",
            "SELECT", "FROM", "WHERE", "GROUP", "BY", "HAVING", "ORDER", "JOIN", "ON",
            "UNION", "ALL", "DISTINCT", "TOP"
        ];

        public HashSet<string> Columns { get; } = new(StringComparer.OrdinalIgnoreCase);

        public override void Visit(ColumnReferenceExpression node)
        {
            // Extract the column name from qualified or unqualified references
            if (node.MultiPartIdentifier != null && node.MultiPartIdentifier.Identifiers.Count > 0)
            {
                // Get the last identifier (column name)
                var columnName = node.MultiPartIdentifier.Identifiers[^1].Value;
                if (!IsSystemOrReserved(columnName))
                {
                    Columns.Add(columnName);
                }
            }

            base.Visit(node);
        }

        public override void Visit(SelectScalarExpression node)
        {
            // Handle SELECT expressions (columns, functions, CASE, subqueries, etc.)
            // The visitor will recursively pick up all column references
            AddAliasFromSelectScalarExpression(node);
            base.Visit(node);
        }

        public override void Visit(SelectSetVariable node)
        {
            // Handle @variable assignments
            base.Visit(node);
        }

        public override void Visit(BinaryExpression node)
        {
            // This handles WHERE, JOIN ON, and other binary expressions
            base.Visit(node);
        }

        public override void Visit(FunctionCall node)
        {
            // Handle functions like COUNT(ColumnName), SUM(ColumnName), YEAR(DateColumn), etc.
            // The visitor will automatically handle column references inside functions
            base.Visit(node);
        }

        public override void Visit(TableReferenceWithAlias node)
        {
            // Skip processing the table reference itself
            // but continue with joins and other references
            base.Visit(node);
        }

        public override void Visit(CommonTableExpression node)
        {
            // Handle CTEs - continue walking to find columns in the CTE body
            base.Visit(node);
        }

        public override void Visit(QuerySpecification node)
        {
            // Handle SELECT statements
            base.Visit(node);
        }

        public override void Visit(CaseExpression node)
        {
            // Handle CASE WHEN expressions - column references inside are caught by ColumnReferenceExpression
            base.Visit(node);
        }

        private void AddAliasFromSelectScalarExpression(SelectScalarExpression node)
        {
            var alias = node.ColumnName?.Value;
            if (string.IsNullOrWhiteSpace(alias) || IsSystemOrReserved(alias))
            {
                return;
            }

            Columns.Add(alias);
        }

        /// <summary>
        /// Check if an identifier is a system/reserved word that shouldn't be treated as a column.
        /// </summary>
        internal static bool IsSystemOrReserved(string identifier)
        {
            if (string.IsNullOrWhiteSpace(identifier))
                return true;

            // Common single-letter table aliases
            if (identifier.Length == 1 && char.IsLetter(identifier[0]))
            {
                return true;
            }

            return Reserved.Contains(identifier);
        }
    }
}
