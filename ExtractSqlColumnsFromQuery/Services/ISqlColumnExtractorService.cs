namespace ExtractSqlColumnsFromQuery.Services;

public interface ISqlColumnExtractorService
{
    /// <summary>
    /// Extracts all column names from a raw SQL query string.
    /// Performs static parsing only - does not execute the SQL.
    /// </summary>
    /// <param name="sqlQuery">The raw SQL query string</param>
    /// <returns>Deduplicated column names (case-insensitive)</returns>
    IEnumerable<string> ExtractColumns(string sqlQuery);
}
