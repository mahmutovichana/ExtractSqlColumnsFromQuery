# ExtractSqlColumnsFromQuery

Modern .NET 10 Web API + Blazor test UI for extracting column names from raw SQL text.

## Highlights

- .NET 10 Web API (`net10.0`)
- Static SQL parsing with `Microsoft.SqlServer.TransactSql.ScriptDom`
- Safe fallback extraction for mixed/non-T-SQL fragments
- Swagger endpoint for API testing
- Blazor live UI with SQL textarea + one-click copy outputs
- Comprehensive test suite (complex + extreme + fuzz scenarios)

<img width="2559" height="1391" alt="image" src="https://github.com/user-attachments/assets/a263c8c6-62cf-4191-bf81-27495e5d8b50" />

## Endpoints

- API: `POST /api/sqlparser/extract-columns`
- Swagger UI: `/swagger`
- Live Test UI (Blazor): `/test`

Root route (`/`) redirects to `/test`.

## Request / Response

### Request body

```json
{
  "sql": "SELECT a.AccountId, a.CustomerId FROM Accounts a"
}
```

### Response body

```json
{
  "columns": ["AccountId", "CustomerId"]
}
```

## Run locally

```bash
dotnet restore
dotnet build ExtractSqlColumnsFromQuery.sln
dotnet run --project ExtractSqlColumnsFromQuery/ExtractSqlColumnsFromQuery.csproj
```

Then open:

- http://localhost:5205/test
- http://localhost:5205/swagger

## Blazor Test UI

The test page is optimized for direct paste from SSMS/ADS:

- Multiline SQL paste
- No manual JSON escaping required
- Extract button to run parser immediately
- Copy as list / copy as CSV buttons
- Inline help describing supported SQL features

## Project structure

- `ExtractSqlColumnsFromQuery/` - API + Blazor host
- `ExtractSqlColumnsFromQuery.Tests/` - xUnit tests
- `ExtractSqlColumnsFromQuery.sln` - solution file

## Notes

- SQL text is parsed; it is **not executed**.
- Output is deduplicated and case-insensitive.
- Mixed dialect constructs (e.g., `WITH RECURSIVE`, `LIMIT`) are handled via fallback logic when needed.
