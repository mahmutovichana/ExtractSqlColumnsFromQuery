using ExtractSqlColumnsFromQuery.DTOs;
using ExtractSqlColumnsFromQuery.Services;
using Microsoft.AspNetCore.Mvc;

namespace ExtractSqlColumnsFromQuery.Controllers;

/// <summary>
/// Provides endpoints for parsing and analyzing SQL queries.
/// </summary>
[ApiController]
[Route("api/sqlparser")]
public class SqlParserController : ControllerBase
{
    private readonly ISqlColumnExtractorService _columnExtractorService;
    private readonly ILogger<SqlParserController> _logger;

    public SqlParserController(
        ISqlColumnExtractorService columnExtractorService,
        ILogger<SqlParserController> logger
    )
    {
        _columnExtractorService =
            columnExtractorService
            ?? throw new ArgumentNullException(nameof(columnExtractorService));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <summary>
    /// Extracts all column names from a raw SQL query string.
    /// </summary>
    /// <remarks>
    /// Parses the SQL statically (does not execute it) and returns all column names found.
    /// Handles complex queries with JOINs, subqueries, CTEs, aliases, and derived tables.
    /// </remarks>
    /// <param name="request">The SQL query to parse</param>
    /// <returns>A list of deduplicated column names</returns>
    [HttpPost("extract-columns")]
    [ProducesResponseType(typeof(SqlParseResponse), StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status400BadRequest)]
    [ProducesResponseType(StatusCodes.Status500InternalServerError)]
    public ActionResult<SqlParseResponse> ExtractColumns([FromBody] SqlParseRequest request)
    {
        try
        {
            var columns = _columnExtractorService.ExtractColumns(request.Sql);
            var response = new SqlParseResponse { Columns = columns.ToList() };

            return Ok(response);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error extracting columns from SQL query");
            return Problem(
                title: "Failed to parse SQL query",
                detail: "Unexpected error occurred while processing SQL text.",
                statusCode: StatusCodes.Status500InternalServerError
            );
        }
    }
}
