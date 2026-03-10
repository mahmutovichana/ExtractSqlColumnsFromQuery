using System.ComponentModel.DataAnnotations;

namespace ExtractSqlColumnsFromQuery.DTOs;

public class SqlParseRequest
{
    /// <summary>
    /// The raw SQL query string to parse for column extraction.
    /// </summary>
    [Required(AllowEmptyStrings = false)]
    [MinLength(1)]
    public string Sql { get; set; } = string.Empty;
}
