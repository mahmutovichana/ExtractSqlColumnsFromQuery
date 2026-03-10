namespace ExtractSqlColumnsFromQuery.DTOs;

public class SqlParseResponse
{
    /// <summary>
    /// List of deduplicated column names extracted from the SQL query.
    /// </summary>
    public List<string> Columns { get; set; } = [];
}
