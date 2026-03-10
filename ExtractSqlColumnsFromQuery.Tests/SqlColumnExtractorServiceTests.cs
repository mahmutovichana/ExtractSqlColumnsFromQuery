using ExtractSqlColumnsFromQuery.Services;
using Xunit;

namespace ExtractSqlColumnsFromQuery.Tests;

public class SqlColumnExtractorServiceTests
{
    private readonly SqlColumnExtractorService _service = new();

    [Fact]
    public void ExtractColumns_MultilineCteAndJoins_ReturnsExpectedColumnsAndAliases()
    {
        const string sql =
            @"
WITH OrderCTE AS
(
    SELECT
        o.OrderId,
        o.CustomerId,
        od.ProductId,
        od.Quantity,
        p.Price
    FROM Orders o
    INNER JOIN OrderDetails od ON o.OrderId = od.OrderId
    LEFT JOIN Products p ON od.ProductId = p.ProductId
    WHERE o.OrderDate > '2024-01-01'
),
CustomerOrderSummary AS
(
    SELECT
        c.CustomerId,
        c.CustomerName,
        COUNT(DISTINCT oc.OrderId) AS TotalOrders,
        SUM(oc.Quantity * oc.Price) AS TotalAmount
    FROM OrderCTE oc
    INNER JOIN Customers c ON oc.CustomerId = c.CustomerId
    GROUP BY c.CustomerId, c.CustomerName
)
SELECT
    cos.CustomerId,
    cos.CustomerName,
    cos.TotalOrders,
    cos.TotalAmount,
    (
        SELECT AVG(p.Price)
        FROM Products p
        WHERE p.CategoryId IN
        (
            SELECT DISTINCT od.CategoryId
            FROM OrderDetails od
            WHERE od.CustomerId = cos.CustomerId
        )
    ) AS AvgCategoryPrice
FROM CustomerOrderSummary cos
WHERE cos.TotalAmount >
(
    SELECT AVG(TotalAmount)
    FROM CustomerOrderSummary
);
";

        var columns = _service.ExtractColumns(sql).ToHashSet(StringComparer.OrdinalIgnoreCase);

        Assert.Contains("OrderId", columns);
        Assert.Contains("CustomerId", columns);
        Assert.Contains("ProductId", columns);
        Assert.Contains("Quantity", columns);
        Assert.Contains("Price", columns);
        Assert.Contains("OrderDate", columns);
        Assert.Contains("CategoryId", columns);
        Assert.Contains("TotalOrders", columns);
        Assert.Contains("TotalAmount", columns);
        Assert.Contains("AvgCategoryPrice", columns);
    }

    [Fact]
    public void ExtractColumns_MultilineWindowAndCase_ReturnsColumns()
    {
        const string sql =
            @"
SELECT
    e.EmployeeId,
    e.EmployeeName,
    e.Salary,
    e.DepartmentId,
    (
        SELECT d.DepartmentName
        FROM Departments d
        WHERE d.DepartmentId = e.DepartmentId
    ) AS DeptName,
    ROW_NUMBER() OVER (PARTITION BY e.DepartmentId ORDER BY e.Salary DESC) AS SalaryRank,
    CASE
        WHEN e.Salary >
        (
            SELECT AVG(Salary)
            FROM Employees
            WHERE DepartmentId = e.DepartmentId
        ) THEN 'Above Average'
        ELSE 'Below Average'
    END AS SalaryStatus,
    (
        SELECT COUNT(*)
        FROM EmployeeProjects ep
        WHERE ep.EmployeeId = e.EmployeeId
    ) AS ProjectCount
FROM Employees e
WHERE e.Salary IN
(
    SELECT MAX(Salary)
    FROM Employees
    GROUP BY DepartmentId
)
ORDER BY e.DepartmentId, SalaryRank;
";

        var columns = _service.ExtractColumns(sql).ToHashSet(StringComparer.OrdinalIgnoreCase);

        Assert.Contains("EmployeeId", columns);
        Assert.Contains("EmployeeName", columns);
        Assert.Contains("Salary", columns);
        Assert.Contains("DepartmentId", columns);
        Assert.Contains("DepartmentName", columns);
        Assert.Contains("DeptName", columns);
        Assert.Contains("SalaryRank", columns);
        Assert.Contains("SalaryStatus", columns);
        Assert.Contains("ProjectCount", columns);
    }

    [Fact]
    public void ExtractColumns_MultilineCorrelatedSubqueries_ReturnsColumns()
    {
        const string sql =
            @"
SELECT
    c.CustomerId,
    c.CustomerName,
    c.Email,
    c.City,
    c.Country,
    (
        SELECT COUNT(*)
        FROM Orders o
        WHERE o.CustomerId = c.CustomerId
    ) AS OrderCount,
    (
        SELECT MAX(o.OrderDate)
        FROM Orders o
        WHERE o.CustomerId = c.CustomerId
    ) AS LastOrderDate,
    (
        SELECT SUM(od.Quantity * od.UnitPrice)
        FROM Orders o
        INNER JOIN OrderDetails od ON o.OrderId = od.OrderId
        WHERE o.CustomerId = c.CustomerId
    ) AS LifetimeValue,
    CASE
        WHEN EXISTS
        (
            SELECT 1
            FROM Orders o
            WHERE o.CustomerId = c.CustomerId
              AND o.OrderDate > DATEADD(MONTH, -3, GETDATE())
        ) THEN 'Active'
        ELSE 'Inactive'
    END AS Status
FROM Customers c
WHERE c.CustomerId IN
(
    SELECT DISTINCT o.CustomerId
    FROM Orders o
    WHERE o.OrderDate > '2023-01-01'
      AND EXISTS
      (
        SELECT 1
        FROM OrderDetails od
        WHERE od.OrderId = o.OrderId
          AND od.Quantity > 5
      )
);
";

        var columns = _service.ExtractColumns(sql).ToHashSet(StringComparer.OrdinalIgnoreCase);

        Assert.Contains("CustomerId", columns);
        Assert.Contains("CustomerName", columns);
        Assert.Contains("Email", columns);
        Assert.Contains("City", columns);
        Assert.Contains("Country", columns);
        Assert.Contains("OrderDate", columns);
        Assert.Contains("OrderId", columns);
        Assert.Contains("Quantity", columns);
        Assert.Contains("UnitPrice", columns);
        Assert.Contains("OrderCount", columns);
        Assert.Contains("LastOrderDate", columns);
        Assert.Contains("LifetimeValue", columns);
        Assert.Contains("Status", columns);
    }

    [Fact]
    public void ExtractColumns_WithNonTsqlRecursiveKeyword_UsesFallbackAndReturnsColumns()
    {
        const string sql =
            @"
WITH RECURSIVE CategoryHierarchy AS
(
    SELECT c.CategoryId, c.CategoryName, c.ParentCategoryId
    FROM Categories c
)
SELECT
    ch.CategoryId,
    ch.CategoryName
FROM CategoryHierarchy ch;
";

        var columns = _service.ExtractColumns(sql).ToHashSet(StringComparer.OrdinalIgnoreCase);

        Assert.Contains("CategoryId", columns);
        Assert.Contains("CategoryName", columns);
        Assert.Contains("ParentCategoryId", columns);
    }

    [Theory]
    [MemberData(nameof(ComplexSqlCases))]
    public void ExtractColumns_ComplexQueries_ReturnsExpectedCoreColumns(
        string sql,
        string[] expectedColumns
    )
    {
        var columns = _service.ExtractColumns(sql).ToHashSet(StringComparer.OrdinalIgnoreCase);

        Assert.NotEmpty(columns);
        foreach (var expected in expectedColumns)
        {
            Assert.Contains(expected, columns);
        }
    }

    public static IEnumerable<object[]> ComplexSqlCases()
    {
        yield return Case(
            """
            WITH X AS
            (
                SELECT a.AccountId, a.CustomerId, t.Amount, t.PostingDate
                FROM Accounts a
                JOIN Transactions t ON t.AccountId = a.AccountId
            )
            SELECT x.AccountId, x.CustomerId, SUM(x.Amount) AS TotalAmount
            FROM X x
            GROUP BY x.AccountId, x.CustomerId;
            """,
            "AccountId",
            "CustomerId",
            "Amount",
            "TotalAmount"
        );

        yield return Case(
            """
            SELECT c.CustomerId, c.CustomerName, d.LatestAmount
            FROM Customers c
            CROSS APPLY
            (
                SELECT TOP 1 p.Amount AS LatestAmount
                FROM Payments p
                WHERE p.CustomerId = c.CustomerId
                ORDER BY p.PaymentDate DESC
            ) d;
            """,
            "CustomerId",
            "CustomerName",
            "Amount",
            "LatestAmount"
        );

        yield return Case(
            """
            SELECT c.CustomerId, oa.LastInvoiceDate
            FROM Customers c
            OUTER APPLY
            (
                SELECT MAX(i.InvoiceDate) AS LastInvoiceDate
                FROM Invoices i
                WHERE i.CustomerId = c.CustomerId
            ) oa;
            """,
            "CustomerId",
            "InvoiceDate",
            "LastInvoiceDate"
        );

        yield return Case(
            """
            SELECT
                t.AccountId,
                t.PostingDate,
                t.Amount,
                LAG(t.Amount,1,0) OVER(PARTITION BY t.AccountId ORDER BY t.PostingDate) AS PrevAmount,
                LEAD(t.Amount,1,0) OVER(PARTITION BY t.AccountId ORDER BY t.PostingDate) AS NextAmount
            FROM Transactions t;
            """,
            "AccountId",
            "PostingDate",
            "Amount",
            "PrevAmount",
            "NextAmount"
        );

        yield return Case(
            """
            SELECT
                e.EmployeeId,
                e.DepartmentId,
                DENSE_RANK() OVER(PARTITION BY e.DepartmentId ORDER BY e.Salary DESC) AS SalaryDenseRank
            FROM Employees e;
            """,
            "EmployeeId",
            "DepartmentId",
            "Salary",
            "SalaryDenseRank"
        );

        yield return Case(
            """
            SELECT
                i.InvoiceId,
                CASE WHEN i.TotalAmount > 10000 THEN 'HIGH' ELSE 'LOW' END AS RiskLevel,
                COALESCE(i.CurrencyCode,'BAM') AS EffectiveCurrency
            FROM Invoices i;
            """,
            "InvoiceId",
            "TotalAmount",
            "CurrencyCode",
            "RiskLevel",
            "EffectiveCurrency"
        );

        yield return Case(
            """
            SELECT x.CustomerId, x.NetAmount
            FROM
            (
                SELECT o.CustomerId, SUM(od.Quantity * od.UnitPrice) AS NetAmount
                FROM Orders o
                JOIN OrderDetails od ON od.OrderId = o.OrderId
                GROUP BY o.CustomerId
            ) x
            WHERE x.NetAmount > 1000;
            """,
            "CustomerId",
            "Quantity",
            "UnitPrice",
            "OrderId",
            "NetAmount"
        );

        yield return Case(
            """
            SELECT a.AccountId, a.Balance
            FROM Accounts a
            WHERE EXISTS
            (
                SELECT 1
                FROM Transactions t
                WHERE t.AccountId = a.AccountId
                  AND t.Amount > 500
            );
            """,
            "AccountId",
            "Balance",
            "Amount"
        );

        yield return Case(
            """
            SELECT a.AccountId
            FROM Accounts a
            WHERE NOT EXISTS
            (
                SELECT 1
                FROM AccountLocks l
                WHERE l.AccountId = a.AccountId
                  AND l.IsActive = 1
            );
            """,
            "AccountId",
            "IsActive"
        );

        yield return Case(
            """
            SELECT p.ProductId, p.ProductName
            FROM Products p
            WHERE p.ProductId IN
            (
                SELECT od.ProductId
                FROM OrderDetails od
                GROUP BY od.ProductId
                HAVING SUM(od.Quantity) > 50
            );
            """,
            "ProductId",
            "ProductName",
            "Quantity"
        );

        yield return Case(
            """
            SELECT DISTINCT c.CustomerId, c.CustomerName
            FROM Customers c
            JOIN Orders o ON o.CustomerId = c.CustomerId
            WHERE o.OrderDate BETWEEN '2024-01-01' AND '2024-12-31';
            """,
            "CustomerId",
            "CustomerName",
            "OrderDate"
        );

        yield return Case(
            """
            SELECT TOP 100 WITH TIES t.AccountId, t.Amount
            FROM Transactions t
            ORDER BY t.Amount DESC;
            """,
            "AccountId",
            "Amount"
        );

        yield return Case(
            """
            SELECT t.AccountId, t.PostingDate, t.Amount
            FROM Transactions t
            ORDER BY t.PostingDate DESC
            OFFSET 10 ROWS FETCH NEXT 20 ROWS ONLY;
            """,
            "AccountId",
            "PostingDate",
            "Amount"
        );

        yield return Case(
            """
            SELECT
                o.OrderId,
                SUM(CASE WHEN od.Quantity > 10 THEN od.Quantity ELSE 0 END) AS BigQty,
                SUM(CASE WHEN od.Quantity <= 10 THEN od.Quantity ELSE 0 END) AS SmallQty
            FROM Orders o
            JOIN OrderDetails od ON od.OrderId = o.OrderId
            GROUP BY o.OrderId;
            """,
            "OrderId",
            "Quantity",
            "BigQty",
            "SmallQty"
        );

        yield return Case(
            """
            SELECT
                e.EmployeeId,
                e.EmployeeName,
                (SELECT COUNT(*) FROM EmployeeProjects ep WHERE ep.EmployeeId = e.EmployeeId) AS ProjectCount
            FROM Employees e;
            """,
            "EmployeeId",
            "EmployeeName",
            "ProjectCount"
        );

        yield return Case(
            """
            WITH S AS
            (
                SELECT s.SalesId, s.EmployeeId, s.SalesAmount, YEAR(s.SalesDate) AS SalesYear
                FROM Sales s
            )
            SELECT S.EmployeeId, S.SalesYear, SUM(S.SalesAmount) AS TotalSales
            FROM S
            GROUP BY S.EmployeeId, S.SalesYear;
            """,
            "SalesId",
            "EmployeeId",
            "SalesAmount",
            "SalesDate",
            "SalesYear",
            "TotalSales"
        );

        yield return Case(
            """
            SELECT a.AccountId, a.CustomerId
            INTO #ActiveAccounts
            FROM Accounts a
            WHERE a.Status = 'ACTIVE';

            SELECT aa.AccountId, aa.CustomerId
            FROM #ActiveAccounts aa;
            """,
            "AccountId",
            "CustomerId",
            "Status"
        );

        yield return Case(
            """
            UPDATE a
            SET a.Balance = a.Balance + t.Amount
            FROM Accounts a
            JOIN Transactions t ON t.AccountId = a.AccountId
            WHERE t.PostingDate >= '2025-01-01';
            """,
            "Balance",
            "Amount",
            "AccountId",
            "PostingDate"
        );

        yield return Case(
            """
            DELETE o
            FROM Orders o
            WHERE EXISTS
            (
                SELECT 1
                FROM OrderFlags f
                WHERE f.OrderId = o.OrderId
                  AND f.FlagCode = 'FRAUD'
            );
            """,
            "OrderId",
            "FlagCode"
        );

        yield return Case(
            """
            INSERT INTO AuditLog(EntityId, EntityType, CreatedAt)
            SELECT c.CustomerId, 'Customer', GETDATE()
            FROM Customers c
            WHERE c.IsVip = 1;
            """,
            "EntityId",
            "EntityType",
            "CreatedAt",
            "CustomerId",
            "IsVip"
        );

        yield return Case(
            """
            MERGE CustomerSnapshot AS t
            USING Customers AS s
            ON t.CustomerId = s.CustomerId
            WHEN MATCHED THEN
                UPDATE SET t.CustomerName = s.CustomerName, t.Email = s.Email
            WHEN NOT MATCHED THEN
                INSERT (CustomerId, CustomerName, Email)
                VALUES (s.CustomerId, s.CustomerName, s.Email);
            """,
            "CustomerId",
            "CustomerName",
            "Email"
        );

        yield return Case(
            """
            SELECT p.ProductId, p.ProductName, SUM(s.Quantity) AS SoldQty
            FROM Products p
            LEFT JOIN StockMovements s ON s.ProductId = p.ProductId
            GROUP BY GROUPING SETS
            (
                (p.ProductId, p.ProductName),
                ()
            );
            """,
            "ProductId",
            "ProductName",
            "Quantity",
            "SoldQty"
        );

        yield return Case(
            """
            SELECT
                t.AccountId,
                DATEFROMPARTS(YEAR(t.PostingDate), MONTH(t.PostingDate), 1) AS MonthStart,
                SUM(t.Amount) AS MonthAmount
            FROM Transactions t
            GROUP BY t.AccountId, YEAR(t.PostingDate), MONTH(t.PostingDate);
            """,
            "AccountId",
            "PostingDate",
            "Amount",
            "MonthStart",
            "MonthAmount"
        );

        yield return Case(
            """
            SELECT
                e.EmployeeId,
                e.DepartmentId,
                SUM(e.Salary) OVER(PARTITION BY e.DepartmentId) AS DepartmentPayroll
            FROM Employees e;
            """,
            "EmployeeId",
            "DepartmentId",
            "Salary",
            "DepartmentPayroll"
        );

        yield return Case(
            """
            SELECT e.EmployeeId, e.Salary,
                   PERCENT_RANK() OVER(PARTITION BY e.DepartmentId ORDER BY e.Salary) AS SalaryPercentile
            FROM Employees e;
            """,
            "EmployeeId",
            "Salary",
            "DepartmentId",
            "SalaryPercentile"
        );

        yield return Case(
            """
            SELECT j.CustomerId,
                   JSON_VALUE(j.Payload, '$.risk.score') AS RiskScore,
                   JSON_VALUE(j.Payload, '$.risk.segment') AS RiskSegment
            FROM CustomerJson j;
            """,
            "CustomerId",
            "Payload",
            "RiskScore",
            "RiskSegment"
        );

        yield return Case(
            """
            SELECT j.CustomerId, d.[key] AS AttrKey, d.[value] AS AttrValue
            FROM CustomerJson j
            CROSS APPLY OPENJSON(j.Payload, '$.attributes') d;
            """,
            "CustomerId",
            "Payload",
            "key",
            "value",
            "AttrKey",
            "AttrValue"
        );

        yield return Case(
            """
            SELECT a.AccountId, s.value AS RoleCode
            FROM Accounts a
            CROSS APPLY STRING_SPLIT(a.RoleCodes, ',') s;
            """,
            "AccountId",
            "RoleCodes",
            "value",
            "RoleCode"
        );

        yield return Case(
            """
            SELECT x.AccountId, x.EventDate
            FROM
            (
                SELECT AccountId, PostingDate AS EventDate FROM Transactions
                UNION ALL
                SELECT AccountId, CreatedAt  AS EventDate FROM AccountAudit
            ) x;
            """,
            "AccountId",
            "PostingDate",
            "CreatedAt",
            "EventDate"
        );

        yield return Case(
            """
            SELECT AccountId FROM ActiveAccounts
            INTERSECT
            SELECT AccountId FROM MonitoredAccounts;
            """,
            "AccountId"
        );

        yield return Case(
            """
            SELECT AccountId FROM ActiveAccounts
            EXCEPT
            SELECT AccountId FROM ClosedAccounts;
            """,
            "AccountId"
        );

        yield return Case(
            """
            WITH CTE AS
            (
                SELECT EmployeeId, ManagerId, 0 AS Lvl
                FROM Employees
                WHERE ManagerId IS NULL
                UNION ALL
                SELECT e.EmployeeId, e.ManagerId, c.Lvl + 1
                FROM Employees e
                JOIN CTE c ON c.EmployeeId = e.ManagerId
            )
            SELECT EmployeeId, ManagerId, Lvl
            FROM CTE;
            """,
            "EmployeeId",
            "ManagerId",
            "Lvl"
        );

        yield return Case(
            """
            SELECT b.BranchId,
                   SUM(CASE WHEN t.Amount > 0 THEN t.Amount ELSE 0 END) AS CreditAmount,
                   SUM(CASE WHEN t.Amount < 0 THEN t.Amount ELSE 0 END) AS DebitAmount
            FROM Branches b
            JOIN Accounts a ON a.BranchId = b.BranchId
            JOIN Transactions t ON t.AccountId = a.AccountId
            GROUP BY b.BranchId;
            """,
            "BranchId",
            "Amount",
            "AccountId",
            "CreditAmount",
            "DebitAmount"
        );

        yield return Case(
            """
            SELECT
                p.PolicyId,
                p.CustomerId,
                IIF(p.Premium > 1000, 1, 0) AS IsHighPremium
            FROM Policies p;
            """,
            "PolicyId",
            "CustomerId",
            "Premium",
            "IsHighPremium"
        );

        yield return Case(
            """
            SELECT
                tr.TradeId,
                tr.BookId,
                SUM(tr.Notional) OVER(PARTITION BY tr.BookId ORDER BY tr.TradeDate ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS RunningNotional
            FROM Trades tr;
            """,
            "TradeId",
            "BookId",
            "Notional",
            "TradeDate",
            "RunningNotional"
        );

        yield return Case(
            """
            SELECT
                c.CustomerId,
                c.[CustomerName],
                c.[RiskScore]
            FROM [dbo].[Customers] c
            WHERE c.[RiskScore] >= 80;
            """,
            "CustomerId",
            "CustomerName",
            "RiskScore"
        );

        yield return Case(
            """
            DECLARE @StartDate DATE = '2025-01-01';
            SELECT t.AccountId, t.PostingDate, t.Amount
            FROM Transactions t
            WHERE t.PostingDate >= @StartDate;
            """,
            "AccountId",
            "PostingDate",
            "Amount"
        );

        yield return Case(
            """
            SELECT
                c.CustomerId,
                COUNT(DISTINCT o.OrderId) AS DistinctOrders,
                AVG(od.UnitPrice) AS AvgPrice
            FROM Customers c
            LEFT JOIN Orders o ON o.CustomerId = c.CustomerId
            LEFT JOIN OrderDetails od ON od.OrderId = o.OrderId
            GROUP BY c.CustomerId;
            """,
            "CustomerId",
            "OrderId",
            "UnitPrice",
            "DistinctOrders",
            "AvgPrice"
        );

        yield return Case(
            """
            SELECT
                p.PortfolioId,
                SUM(CASE WHEN ps.Side = 'BUY' THEN ps.Quantity ELSE -ps.Quantity END) AS NetQty
            FROM Positions ps
            JOIN Portfolios p ON p.PortfolioId = ps.PortfolioId
            GROUP BY p.PortfolioId
            HAVING SUM(ABS(ps.Quantity)) > 0;
            """,
            "PortfolioId",
            "Side",
            "Quantity",
            "NetQty"
        );

        yield return Case(
            """
            SELECT
                a.AccountId,
                MAX(CASE WHEN af.FlagCode = 'AML' THEN 1 ELSE 0 END) AS HasAmlFlag
            FROM Accounts a
            LEFT JOIN AccountFlags af ON af.AccountId = a.AccountId
            GROUP BY a.AccountId;
            """,
            "AccountId",
            "FlagCode",
            "HasAmlFlag"
        );

        yield return Case(
            """
            SELECT
                r.ReportId,
                r.CustomerId,
                r.CreatedAt,
                DATENAME(MONTH, r.CreatedAt) AS CreatedMonthName
            FROM Reports r;
            """,
            "ReportId",
            "CustomerId",
            "CreatedAt",
            "CreatedMonthName"
        );

        yield return Case(
            """
            SELECT
                x.CustomerId,
                x.MaxTxn
            FROM
            (
                SELECT t.CustomerId, MAX(t.Amount) AS MaxTxn
                FROM Transactions t
                GROUP BY t.CustomerId
            ) x
            WHERE x.MaxTxn >
            (
                SELECT AVG(t2.Amount)
                FROM Transactions t2
                WHERE t2.CustomerId = x.CustomerId
            );
            """,
            "CustomerId",
            "Amount",
            "MaxTxn"
        );

        yield return Case(
            """
            WITH RECURSIVE R AS
            (
                SELECT n.NodeId, n.ParentNodeId, n.NodeName
                FROM Nodes n
            )
            SELECT r.NodeId, r.ParentNodeId, r.NodeName
            FROM R r;
            """,
            "NodeId",
            "ParentNodeId",
            "NodeName"
        );
    }

    [Fact]
    public void ExtractColumns_InvalidSql_CompletelyBroken_ReturnsEmptyOrSafeResult()
    {
        const string sql = "SELECT FROM WHERE ;;; this is broken";

        var columns = _service.ExtractColumns(sql).ToList();

        Assert.NotNull(columns);
        Assert.DoesNotContain("SELECT", columns, StringComparer.OrdinalIgnoreCase);
        Assert.DoesNotContain("FROM", columns, StringComparer.OrdinalIgnoreCase);
    }

    [Fact]
    public void ExtractColumns_InvalidButRecoverableSql_FallbackStillFindsColumns()
    {
        const string sql =
            @"
WITH RECURSIVE bad_cte AS
(
    SELECT x.CustomerId, x.OrderDate, x.TotalAmount
    FROM CustomerOrders x
    WHERE x.RegionCode = 'BA'
)
SELECT bad.CustomerId, bad.TotalAmount, bad.OrderDate
FROM bad_cte bad
LIMIT 100;
";

        var columns = _service.ExtractColumns(sql).ToHashSet(StringComparer.OrdinalIgnoreCase);

        Assert.Contains("CustomerId", columns);
        Assert.Contains("OrderDate", columns);
        Assert.Contains("TotalAmount", columns);
        Assert.Contains("RegionCode", columns);
    }

    [Fact]
    public void ExtractColumns_LargeMultilineQuery_ComplianceScenario_ReturnsCoreColumns()
    {
        const string sql = """
            WITH BaseAccounts AS
            (
                SELECT
                    a.AccountId,
                    a.CustomerId,
                    a.BranchId,
                    a.OpenDate,
                    a.CloseDate,
                    a.StatusCode,
                    a.CurrencyCode,
                    a.RiskClass
                FROM Accounts a
                WHERE a.OpenDate >= '2018-01-01'
            ),
            TxnWindow AS
            (
                SELECT
                    t.TransactionId,
                    t.AccountId,
                    t.PostingDate,
                    t.ValueDate,
                    t.Amount,
                    t.DebitCreditCode,
                    t.ChannelCode,
                    t.CounterpartyCountry,
                    t.CounterpartyBank,
                    ROW_NUMBER() OVER (PARTITION BY t.AccountId ORDER BY t.PostingDate DESC) AS TxnRank,
                    SUM(t.Amount) OVER (PARTITION BY t.AccountId ORDER BY t.PostingDate ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS Rolling30Amount
                FROM Transactions t
                WHERE t.PostingDate >= DATEADD(YEAR, -2, GETDATE())
            ),
            SuspiciousSignals AS
            (
                SELECT
                    tw.AccountId,
                    MAX(CASE WHEN ABS(tw.Amount) >= 9000 THEN 1 ELSE 0 END) AS HasHighSingleTxn,
                    MAX(CASE WHEN tw.CounterpartyCountry NOT IN ('BA','HR','RS','SI') THEN 1 ELSE 0 END) AS HasForeignExposure,
                    MAX(CASE WHEN tw.ChannelCode IN ('CRYPTO','WALLET') THEN 1 ELSE 0 END) AS HasDigitalChannel,
                    MAX(CASE WHEN tw.Rolling30Amount >= 25000 THEN 1 ELSE 0 END) AS HasHighRollingVolume
                FROM TxnWindow tw
                GROUP BY tw.AccountId
            ),
            CustomerProfile AS
            (
                SELECT
                    c.CustomerId,
                    c.CustomerType,
                    c.CustomerName,
                    c.DateOfBirth,
                    c.CountryCode,
                    c.PepFlag,
                    c.SanctionFlag,
                    c.OnboardingChannel,
                    c.IndustryCode,
                    c.AnnualIncome,
                    c.ExpectedMonthlyTurnover
                FROM Customers c
            ),
            Joined AS
            (
                SELECT
                    ba.AccountId,
                    ba.CustomerId,
                    ba.BranchId,
                    ba.StatusCode,
                    ba.CurrencyCode,
                    ba.RiskClass,
                    cp.CustomerType,
                    cp.CustomerName,
                    cp.CountryCode,
                    cp.PepFlag,
                    cp.SanctionFlag,
                    cp.AnnualIncome,
                    cp.ExpectedMonthlyTurnover,
                    ss.HasHighSingleTxn,
                    ss.HasForeignExposure,
                    ss.HasDigitalChannel,
                    ss.HasHighRollingVolume,
                    (SELECT COUNT(*) FROM AccountDocuments d WHERE d.AccountId = ba.AccountId AND d.DocumentStatus = 'MISSING') AS MissingDocCount,
                    (SELECT MAX(ae.EventDate) FROM AccountEvents ae WHERE ae.AccountId = ba.AccountId AND ae.EventType = 'AML_ALERT') AS LastAmlAlertDate
                FROM BaseAccounts ba
                INNER JOIN CustomerProfile cp ON cp.CustomerId = ba.CustomerId
                LEFT JOIN SuspiciousSignals ss ON ss.AccountId = ba.AccountId
            )
            SELECT
                j.AccountId,
                j.CustomerId,
                j.CustomerName,
                j.CountryCode,
                j.CurrencyCode,
                j.RiskClass,
                j.MissingDocCount,
                j.LastAmlAlertDate,
                CASE
                    WHEN j.SanctionFlag = 1 THEN 'CRITICAL'
                    WHEN j.PepFlag = 1 AND j.HasForeignExposure = 1 THEN 'HIGH'
                    WHEN j.HasHighSingleTxn = 1 OR j.HasHighRollingVolume = 1 THEN 'MEDIUM'
                    ELSE 'LOW'
                END AS FinalRiskBucket,
                CASE
                    WHEN j.AnnualIncome > 0 THEN (j.ExpectedMonthlyTurnover * 12.0) / j.AnnualIncome
                    ELSE NULL
                END AS TurnoverToIncomeRatio
            FROM Joined j
            WHERE j.StatusCode IN ('ACTIVE','BLOCKED')
            ORDER BY j.AccountId;
            """;

        var columns = _service.ExtractColumns(sql).ToHashSet(StringComparer.OrdinalIgnoreCase);

        Assert.Contains("AccountId", columns);
        Assert.Contains("CustomerId", columns);
        Assert.Contains("TransactionId", columns);
        Assert.Contains("PostingDate", columns);
        Assert.Contains("Amount", columns);
        Assert.Contains("Rolling30Amount", columns);
        Assert.Contains("PepFlag", columns);
        Assert.Contains("SanctionFlag", columns);
        Assert.Contains("MissingDocCount", columns);
        Assert.Contains("LastAmlAlertDate", columns);
        Assert.Contains("FinalRiskBucket", columns);
        Assert.Contains("TurnoverToIncomeRatio", columns);
    }

    [Fact]
    public void ExtractColumns_LargeMultilineQuery_PortfolioStressScenario_ReturnsCoreColumns()
    {
        const string sql = """
            WITH PositionBase AS
            (
                SELECT
                    p.PositionId,
                    p.PortfolioId,
                    p.BookId,
                    p.InstrumentId,
                    p.TradeDate,
                    p.SettleDate,
                    p.Notional,
                    p.MarketValue,
                    p.CurrencyCode,
                    p.RiskFactor,
                    p.Sensitivity,
                    p.Delta,
                    p.Gamma,
                    p.Vega,
                    p.Theta
                FROM Positions p
                WHERE p.TradeDate >= DATEADD(MONTH, -18, GETDATE())
            ),
            InstrumentDim AS
            (
                SELECT
                    i.InstrumentId,
                    i.AssetClass,
                    i.ProductType,
                    i.IssuerId,
                    i.CountryRisk,
                    i.LiquidityBucket,
                    i.MaturityDate,
                    i.CouponRate
                FROM Instruments i
            ),
            ShockScenarios AS
            (
                SELECT 'BASE' AS ScenarioCode, 0.00 AS RateShock, 0.00 AS FxShock, 0.00 AS EquityShock
                UNION ALL SELECT 'RATES_UP_200', 0.02, 0.00, 0.00
                UNION ALL SELECT 'FX_DOWN_10', 0.00, -0.10, 0.00
                UNION ALL SELECT 'EQ_DOWN_25', 0.00, 0.00, -0.25
                UNION ALL SELECT 'COMBINED_STRESS', 0.03, -0.15, -0.30
            ),
            Expanded AS
            (
                SELECT
                    pb.PositionId,
                    pb.PortfolioId,
                    pb.BookId,
                    pb.InstrumentId,
                    pb.Notional,
                    pb.MarketValue,
                    pb.CurrencyCode,
                    pb.RiskFactor,
                    pb.Sensitivity,
                    pb.Delta,
                    pb.Gamma,
                    pb.Vega,
                    pb.Theta,
                    id.AssetClass,
                    id.ProductType,
                    id.CountryRisk,
                    id.LiquidityBucket,
                    ss.ScenarioCode,
                    ss.RateShock,
                    ss.FxShock,
                    ss.EquityShock,
                    (pb.Delta * ss.EquityShock) + (pb.Vega * ss.FxShock) + (pb.Sensitivity * ss.RateShock) AS ShockPnl
                FROM PositionBase pb
                JOIN InstrumentDim id ON id.InstrumentId = pb.InstrumentId
                CROSS JOIN ShockScenarios ss
            ),
            PortfolioAgg AS
            (
                SELECT
                    e.PortfolioId,
                    e.BookId,
                    e.ScenarioCode,
                    SUM(e.Notional) AS TotalNotional,
                    SUM(e.MarketValue) AS TotalMarketValue,
                    SUM(e.ShockPnl) AS ScenarioPnl,
                    MAX(CASE WHEN e.LiquidityBucket IN ('ILLIQ','STRESSED') THEN 1 ELSE 0 END) AS HasLiquidityStress,
                    MAX(CASE WHEN e.CountryRisk IN ('HIGH','VERY_HIGH') THEN 1 ELSE 0 END) AS HasCountryStress,
                    COUNT(DISTINCT e.InstrumentId) AS InstrumentCount
                FROM Expanded e
                GROUP BY e.PortfolioId, e.BookId, e.ScenarioCode
            )
            SELECT
                pa.PortfolioId,
                pa.BookId,
                pa.ScenarioCode,
                pa.TotalNotional,
                pa.TotalMarketValue,
                pa.ScenarioPnl,
                pa.HasLiquidityStress,
                pa.HasCountryStress,
                pa.InstrumentCount,
                CASE
                    WHEN pa.ScenarioPnl < -1000000 THEN 'SEVERE'
                    WHEN pa.ScenarioPnl < -250000 THEN 'ELEVATED'
                    ELSE 'ACCEPTABLE'
                END AS StressLevel,
                ROW_NUMBER() OVER (PARTITION BY pa.PortfolioId ORDER BY pa.ScenarioPnl ASC) AS WorstScenarioRank
            FROM PortfolioAgg pa
            ORDER BY pa.PortfolioId, pa.WorstScenarioRank;
            """;

        var columns = _service.ExtractColumns(sql).ToHashSet(StringComparer.OrdinalIgnoreCase);

        Assert.Contains("PositionId", columns);
        Assert.Contains("PortfolioId", columns);
        Assert.Contains("InstrumentId", columns);
        Assert.Contains("Notional", columns);
        Assert.Contains("MarketValue", columns);
        Assert.Contains("ScenarioCode", columns);
        Assert.Contains("ShockPnl", columns);
        Assert.Contains("ScenarioPnl", columns);
        Assert.Contains("InstrumentCount", columns);
        Assert.Contains("StressLevel", columns);
        Assert.Contains("WorstScenarioRank", columns);
    }

    [Fact]
    public void ExtractColumns_LargeMultilineQuery_WithTempTablesAndMerge_ReturnsCoreColumns()
    {
        const string sql = """
            SELECT
                c.CustomerId,
                c.CustomerName,
                c.CountryCode,
                c.KycStatus,
                c.RiskScore,
                a.AccountId,
                a.StatusCode,
                a.CurrencyCode,
                a.OpenDate,
                SUM(CASE WHEN t.Amount > 0 THEN t.Amount ELSE 0 END) AS TotalCredit,
                SUM(CASE WHEN t.Amount < 0 THEN ABS(t.Amount) ELSE 0 END) AS TotalDebit,
                MAX(t.PostingDate) AS LastPostingDate
            INTO #CustomerAccountSnapshot
            FROM Customers c
            JOIN Accounts a ON a.CustomerId = c.CustomerId
            LEFT JOIN Transactions t ON t.AccountId = a.AccountId
            WHERE a.OpenDate >= '2020-01-01'
            GROUP BY
                c.CustomerId,
                c.CustomerName,
                c.CountryCode,
                c.KycStatus,
                c.RiskScore,
                a.AccountId,
                a.StatusCode,
                a.CurrencyCode,
                a.OpenDate;

            SELECT
                s.CustomerId,
                s.CustomerName,
                s.AccountId,
                s.TotalCredit,
                s.TotalDebit,
                s.LastPostingDate,
                CASE
                    WHEN s.KycStatus <> 'OK' THEN 1
                    WHEN s.RiskScore >= 85 THEN 1
                    WHEN s.TotalCredit > 500000 OR s.TotalDebit > 500000 THEN 1
                    ELSE 0
                END AS NeedsEnhancedReview
            INTO #ReviewCandidates
            FROM #CustomerAccountSnapshot s;

            MERGE ComplianceReviewQueue AS target
            USING #ReviewCandidates AS src
            ON target.CustomerId = src.CustomerId
               AND target.AccountId = src.AccountId
            WHEN MATCHED THEN
                UPDATE SET
                    target.LastPostingDate = src.LastPostingDate,
                    target.TotalCredit = src.TotalCredit,
                    target.TotalDebit = src.TotalDebit,
                    target.NeedsEnhancedReview = src.NeedsEnhancedReview,
                    target.UpdatedAt = GETDATE()
            WHEN NOT MATCHED THEN
                INSERT
                (
                    CustomerId,
                    CustomerName,
                    AccountId,
                    LastPostingDate,
                    TotalCredit,
                    TotalDebit,
                    NeedsEnhancedReview,
                    CreatedAt
                )
                VALUES
                (
                    src.CustomerId,
                    src.CustomerName,
                    src.AccountId,
                    src.LastPostingDate,
                    src.TotalCredit,
                    src.TotalDebit,
                    src.NeedsEnhancedReview,
                    GETDATE()
                );

            SELECT
                q.CustomerId,
                q.CustomerName,
                q.AccountId,
                q.TotalCredit,
                q.TotalDebit,
                q.NeedsEnhancedReview,
                q.CreatedAt,
                q.UpdatedAt
            FROM ComplianceReviewQueue q
            WHERE q.NeedsEnhancedReview = 1
            ORDER BY q.CustomerId, q.AccountId;
            """;

        var columns = _service.ExtractColumns(sql).ToHashSet(StringComparer.OrdinalIgnoreCase);

        Assert.Contains("CustomerId", columns);
        Assert.Contains("CustomerName", columns);
        Assert.Contains("CountryCode", columns);
        Assert.Contains("KycStatus", columns);
        Assert.Contains("RiskScore", columns);
        Assert.Contains("AccountId", columns);
        Assert.Contains("Amount", columns);
        Assert.Contains("TotalCredit", columns);
        Assert.Contains("TotalDebit", columns);
        Assert.Contains("LastPostingDate", columns);
        Assert.Contains("NeedsEnhancedReview", columns);
        Assert.Contains("CreatedAt", columns);
        Assert.Contains("UpdatedAt", columns);
    }

    [Theory]
    [MemberData(nameof(ExtremeSqlCases))]
    public void ExtractColumns_ExtremeMixedQueries_ReturnsExpectedColumns(
        string sql,
        string[] expectedColumns
    )
    {
        var columns = _service.ExtractColumns(sql).ToHashSet(StringComparer.OrdinalIgnoreCase);

        Assert.NotEmpty(columns);
        foreach (var expected in expectedColumns)
        {
            Assert.Contains(expected, columns);
        }
    }

    public static IEnumerable<object[]> ExtremeSqlCases()
    {
        yield return Case(
            """
            CREATE TABLE #TmpA (AccountId INT, CustomerId INT, Amount DECIMAL(18,2), PostingDate DATE);
            INSERT INTO #TmpA(AccountId, CustomerId, Amount, PostingDate)
            SELECT t.AccountId, t.CustomerId, t.Amount, t.PostingDate
            FROM Transactions t
            WHERE t.PostingDate >= DATEADD(DAY, -90, GETDATE());

            SELECT a.AccountId,
                   a.CustomerId,
                   SUM(a.Amount) AS Amount90d,
                   MAX(a.PostingDate) AS LastTxnDate
            FROM #TmpA a
            GROUP BY a.AccountId, a.CustomerId;
            """,
            "AccountId",
            "CustomerId",
            "Amount",
            "PostingDate",
            "Amount90d",
            "LastTxnDate"
        );

        yield return Case(
            """
            WITH Raw AS
            (
                SELECT p.PortfolioId, p.BookId, p.CurrencyCode, p.Notional, p.MarketValue
                FROM Positions p
            ),
            Fx AS
            (
                SELECT f.CurrencyCode, f.RateToEur
                FROM FxRates f
                WHERE f.RateDate = (SELECT MAX(fr.RateDate) FROM FxRates fr)
            )
            SELECT r.PortfolioId,
                   r.BookId,
                   SUM(r.Notional * fx.RateToEur) AS NotionalEur,
                   SUM(r.MarketValue * fx.RateToEur) AS MarketValueEur
            FROM Raw r
            JOIN Fx fx ON fx.CurrencyCode = r.CurrencyCode
            GROUP BY r.PortfolioId, r.BookId;
            """,
            "PortfolioId",
            "BookId",
            "CurrencyCode",
            "Notional",
            "MarketValue",
            "RateToEur",
            "NotionalEur",
            "MarketValueEur"
        );

        yield return Case(
            """
            SELECT x.CustomerId,
                   x.Payload,
                   JSON_VALUE(x.Payload, '$.aml.score') AS AmlScore,
                   JSON_VALUE(x.Payload, '$.aml.reason') AS AmlReason,
                   j.[key] AS AttrKey,
                   j.[value] AS AttrValue
            FROM CustomerJson x
            OUTER APPLY OPENJSON(x.Payload, '$.attributes') j;
            """,
            "CustomerId",
            "Payload",
            "AmlScore",
            "AmlReason",
            "key",
            "value",
            "AttrKey",
            "AttrValue"
        );

        yield return Case(
            """
            SELECT n.CustomerId,
                   n.Email,
                   n.Phone,
                   ROW_NUMBER() OVER(PARTITION BY n.CustomerId ORDER BY n.UpdatedAt DESC) AS rn
            INTO #LatestContacts
            FROM CustomerContacts n;

            SELECT lc.CustomerId, lc.Email, lc.Phone
            FROM #LatestContacts lc
            WHERE lc.rn = 1;
            """,
            "CustomerId",
            "Email",
            "Phone",
            "UpdatedAt",
            "rn"
        );

        yield return Case(
            """
            SELECT *
            FROM
            (
                SELECT t.AccountId,
                       DATENAME(MONTH, t.PostingDate) AS MonthName,
                       ABS(t.Amount) AS AmountAbs
                FROM Transactions t
            ) src
            PIVOT
            (
                SUM(AmountAbs)
                FOR MonthName IN ([January],[February],[March],[April])
            ) p;
            """,
            "AccountId",
            "PostingDate",
            "Amount",
            "MonthName",
            "AmountAbs"
        );

        yield return Case(
            """
            SELECT u.AccountId,
                   u.Category,
                   u.Amount
            FROM
            (
                SELECT p.AccountId, p.CreditAmount, p.DebitAmount
                FROM AccountBalances p
            ) s
            UNPIVOT
            (
                Amount FOR Category IN (CreditAmount, DebitAmount)
            ) u;
            """,
            "AccountId",
            "CreditAmount",
            "DebitAmount",
            "Amount",
            "Category"
        );

        yield return Case(
            """
            SELECT c.CustomerId,
                   c.CustomerName,
                   t.TagValue
            FROM Customers c
            CROSS APPLY STRING_SPLIT(c.TagList, ',') ss
            CROSS APPLY
            (
                SELECT LTRIM(RTRIM(ss.value)) AS TagValue
            ) t;
            """,
            "CustomerId",
            "CustomerName",
            "TagList",
            "value",
            "TagValue"
        );

        yield return Case(
            """
            ;WITH XMLNAMESPACES (DEFAULT 'urn:bank:risk:v1')
            SELECT r.ReportId,
                   r.XmlPayload,
                   x.n.value('(CustomerId/text())[1]', 'int') AS CustomerId,
                   x.n.value('(RiskScore/text())[1]', 'int') AS RiskScore
            FROM RiskXmlReports r
            CROSS APPLY r.XmlPayload.nodes('/Report/Entry') x(n);
            """,
            "ReportId",
            "XmlPayload",
            "CustomerId",
            "RiskScore"
        );

        yield return Case(
            """
            SELECT s.CustomerId,
                   s.Score,
                   NTILE(10) OVER(ORDER BY s.Score DESC) AS Decile,
                   PERCENT_RANK() OVER(ORDER BY s.Score DESC) AS ScorePercentRank,
                   CUME_DIST() OVER(ORDER BY s.Score DESC) AS ScoreCumeDist
            FROM CustomerScores s;
            """,
            "CustomerId",
            "Score",
            "Decile",
            "ScorePercentRank",
            "ScoreCumeDist"
        );

        yield return Case(
            """
            SELECT a.AccountId,
                   a.Balance,
                   NULLIF(a.Balance, 0) AS NonZeroBalance,
                   TRY_CONVERT(decimal(18,2), a.LimitText) AS LimitValue,
                   ISNULL(a.StatusCode, 'UNKNOWN') AS EffectiveStatus
            FROM Accounts a;
            """,
            "AccountId",
            "Balance",
            "NonZeroBalance",
            "LimitText",
            "LimitValue",
            "StatusCode",
            "EffectiveStatus"
        );

        yield return Case(
            """
            SELECT o.OrderId,
                   o.CustomerId,
                   SUM(od.Quantity * od.UnitPrice) AS GrossAmount,
                   SUM(od.Quantity * od.UnitPrice * (1 - od.DiscountRate)) AS NetAmount,
                   SUM(od.TaxAmount) AS TotalTax
            FROM Orders o
            JOIN OrderDetails od ON od.OrderId = o.OrderId
            GROUP BY o.OrderId, o.CustomerId
            HAVING SUM(od.Quantity * od.UnitPrice) > 10000;
            """,
            "OrderId",
            "CustomerId",
            "Quantity",
            "UnitPrice",
            "DiscountRate",
            "TaxAmount",
            "GrossAmount",
            "NetAmount",
            "TotalTax"
        );

        yield return Case(
            """
            DECLARE @Sql nvarchar(max) = N'
            SELECT t.AccountId, t.Amount, t.PostingDate
            FROM Transactions t
            WHERE t.PostingDate >= @P1';
            EXEC sp_executesql @Sql, N'@P1 date', @P1='2025-01-01';
            """,
            "AccountId",
            "Amount",
            "PostingDate"
        );

        yield return Case(
            """
            SELECT b.BranchId,
                   b.BranchName,
                   c.CustomerId,
                   c.CustomerName,
                   a.AccountId,
                   a.CurrencyCode,
                   t.Amount,
                   t.PostingDate,
                   DATEDIFF(DAY, t.PostingDate, GETDATE()) AS DaysSinceTxn
            FROM Branches b
            JOIN Customers c ON c.BranchId = b.BranchId
            JOIN Accounts a ON a.CustomerId = c.CustomerId
            LEFT JOIN Transactions t ON t.AccountId = a.AccountId;
            """,
            "BranchId",
            "BranchName",
            "CustomerId",
            "CustomerName",
            "AccountId",
            "CurrencyCode",
            "Amount",
            "PostingDate",
            "DaysSinceTxn"
        );

        yield return Case(
            """
            WITH A AS
            (
                SELECT AccountId, CustomerId, StatusCode FROM Accounts
            ),
            B AS
            (
                SELECT CustomerId, SUM(Amount) AS TotalAmt
                FROM Transactions
                GROUP BY CustomerId
            )
            SELECT A.AccountId,
                   A.CustomerId,
                   A.StatusCode,
                   B.TotalAmt,
                   CASE WHEN B.TotalAmt > 50000 THEN 'VIP' ELSE 'REG' END AS Segment
            FROM A
            LEFT JOIN B ON B.CustomerId = A.CustomerId;
            """,
            "AccountId",
            "CustomerId",
            "StatusCode",
            "Amount",
            "TotalAmt",
            "Segment"
        );

        yield return Case(
            """
            SELECT DISTINCT
                   e.EmployeeId,
                   e.DepartmentId,
                   FIRST_VALUE(e.Salary) OVER(PARTITION BY e.DepartmentId ORDER BY e.Salary DESC) AS TopSalary,
                   LAST_VALUE(e.Salary) OVER(PARTITION BY e.DepartmentId ORDER BY e.Salary DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS BottomSalary
            FROM Employees e;
            """,
            "EmployeeId",
            "DepartmentId",
            "Salary",
            "TopSalary",
            "BottomSalary"
        );

        yield return Case(
            """
            SELECT
                q.CustomerId,
                q.QuestionCode,
                q.AnswerText,
                HASHBYTES('SHA2_256', CONCAT(q.CustomerId, ':', q.QuestionCode, ':', q.AnswerText)) AS AnswerHash
            FROM KycQuestionnaires q;
            """,
            "CustomerId",
            "QuestionCode",
            "AnswerText",
            "AnswerHash"
        );

        yield return Case(
            """
            SELECT l.LoanId,
                   l.CustomerId,
                   l.PrincipalAmount,
                   l.InterestRate,
                   POWER(1 + l.InterestRate / 12.0, l.TermMonths) AS GrowthFactor,
                   l.PrincipalAmount * POWER(1 + l.InterestRate / 12.0, l.TermMonths) AS FutureValue
            FROM Loans l;
            """,
            "LoanId",
            "CustomerId",
            "PrincipalAmount",
            "InterestRate",
            "TermMonths",
            "GrowthFactor",
            "FutureValue"
        );

        yield return Case(
            """
            SELECT a.AccountId,
                   a.CountryCode,
                   a.Balance,
                   CASE
                      WHEN a.CountryCode IN ('IR','KP','SY') THEN 'SANCTION_RISK'
                      WHEN a.Balance > 1000000 THEN 'LARGE_EXPOSURE'
                      ELSE 'NORMAL'
                   END AS ExposureClass
            FROM Accounts a
            WHERE EXISTS
            (
                SELECT 1
                FROM Alerts al
                WHERE al.AccountId = a.AccountId
                  AND al.AlertDate >= DATEADD(DAY, -30, GETDATE())
            );
            """,
            "AccountId",
            "CountryCode",
            "Balance",
            "ExposureClass",
            "AlertDate"
        );

        yield return Case(
            """
            SELECT c.CustomerId, c.CustomerName, c.IndustryCode
            FROM Customers c
            WHERE c.CustomerId IN (SELECT CustomerId FROM HighRiskCustomers)
            EXCEPT
            SELECT c2.CustomerId, c2.CustomerName, c2.IndustryCode
            FROM Customers c2
            WHERE c2.CustomerId IN (SELECT CustomerId FROM WhitelistedCustomers);
            """,
            "CustomerId",
            "CustomerName",
            "IndustryCode"
        );

        yield return Case(
            """
            WITH RECURSIVE mixed_dialect AS
            (
                SELECT x.NodeId, x.ParentId, x.NodeLabel
                FROM GraphNodes x
            )
            SELECT md.NodeId,
                   md.ParentId,
                   md.NodeLabel,
                   COALESCE(md.NodeLabel, 'N/A') AS EffectiveLabel,
                   CASE WHEN md.ParentId IS NULL THEN 0 ELSE 1 END AS HasParent
            FROM mixed_dialect md
            LIMIT 500;
            """,
            "NodeId",
            "ParentId",
            "NodeLabel",
            "EffectiveLabel",
            "HasParent"
        );
    }

    [Fact]
    public void ExtractColumns_FuzzGeneratedQueries_DoesNotThrowAndFindsExpectedColumns()
    {
        var rng = new Random(20260309);

        for (var i = 0; i < 250; i++)
        {
            var (sql, expectedAnyOf) = BuildFuzzSql(rng, i);

            var ex = Record.Exception(() => _service.ExtractColumns(sql).ToList());
            Assert.Null(ex);

            var columns = _service.ExtractColumns(sql).ToHashSet(StringComparer.OrdinalIgnoreCase);
            Assert.NotEmpty(columns);
            Assert.True(
                expectedAnyOf.Any(c => columns.Contains(c)),
                $"Fuzz case {i} did not contain any expected columns. SQL:\n{sql}"
            );
        }
    }

    [Fact]
    public void ExtractColumns_FuzzGeneratedQueries_IsStableAndDeduplicated()
    {
        var rng = new Random(20260310);

        for (var i = 0; i < 150; i++)
        {
            var (sql, _) = BuildFuzzSql(rng, i + 1000);

            var first = _service.ExtractColumns(sql).ToList();
            var second = _service.ExtractColumns(sql).ToList();

            Assert.Equal(first, second);

            var distinct = first.Distinct(StringComparer.OrdinalIgnoreCase).Count();
            Assert.Equal(distinct, first.Count);
        }
    }

    private static (string Sql, string[] ExpectedAnyOf) BuildFuzzSql(Random rng, int seedTag)
    {
        var selectAliases = new[]
        {
            "RiskBucket",
            "NetAmount",
            "ScoreRank",
            "LastEventDate",
            "ExposureClass",
        };
        var amountExpr =
            rng.Next(0, 2) == 0
                ? "SUM(t.Amount)"
                : "SUM(CASE WHEN t.Amount > 0 THEN t.Amount ELSE 0 END)";

        var alias = selectAliases[rng.Next(selectAliases.Length)];
        var includeRecursive = rng.Next(0, 5) == 0;
        var includeLimit = rng.Next(0, 4) == 0;
        var includeJson = rng.Next(0, 3) == 0;
        var includeApply = rng.Next(0, 3) == 0;

        var cteHead = includeRecursive ? "WITH RECURSIVE Base AS" : "WITH Base AS";
        var limitTail = includeLimit ? "\nLIMIT 500" : string.Empty;

        var jsonSelect = includeJson
            ? ", JSON_VALUE(c.JsonPayload, '$.risk.score') AS JsonRiskScore"
            : string.Empty;

        var applyJoin = includeApply
            ? @"
OUTER APPLY
(
    SELECT MAX(e.EventDate) AS LastEventDate
    FROM AccountEvents e
    WHERE e.AccountId = b.AccountId
) evt"
            : string.Empty;

        var sql = $"""
{cteHead}
(
    SELECT
        a.AccountId,
        a.CustomerId,
        a.StatusCode,
        t.PostingDate,
        t.Amount,
        ROW_NUMBER() OVER(PARTITION BY a.AccountId ORDER BY t.PostingDate DESC) AS Rn,
        {amountExpr} OVER(PARTITION BY a.AccountId) AS NetAmount
    FROM Accounts a
    JOIN Transactions t ON t.AccountId = a.AccountId
    WHERE a.StatusCode IN ('ACTIVE','BLOCKED')
),
Agg AS
(
    SELECT
        b.AccountId,
        b.CustomerId,
        MAX(b.PostingDate) AS LastPostingDate,
        MAX(b.NetAmount) AS NetAmount,
        MAX(CASE WHEN b.Amount > 9000 THEN 1 ELSE 0 END) AS HighTxnFlag
    FROM Base b
    GROUP BY b.AccountId, b.CustomerId
)
SELECT
    ag.AccountId,
    ag.CustomerId,
    ag.LastPostingDate,
    ag.NetAmount,
    DENSE_RANK() OVER(ORDER BY ag.NetAmount DESC) AS ScoreRank,
    CASE
        WHEN ag.HighTxnFlag = 1 THEN 'HIGH'
        WHEN ag.NetAmount > 50000 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS {alias}
    {jsonSelect}
FROM Agg ag
JOIN Customers c ON c.CustomerId = ag.CustomerId
{applyJoin}
WHERE ag.LastPostingDate >= '2024-01-01'
ORDER BY ag.AccountId{limitTail};
-- fuzz-seed: {seedTag}
""";

        var expected = new List<string>
        {
            "AccountId",
            "CustomerId",
            "PostingDate",
            "Amount",
            "LastPostingDate",
            "NetAmount",
            "ScoreRank",
            alias,
        };

        if (includeJson)
        {
            expected.Add("JsonRiskScore");
            expected.Add("JsonPayload");
        }

        if (includeApply)
        {
            expected.Add("EventDate");
            expected.Add("LastEventDate");
        }

        return (sql, expected.ToArray());
    }

    private static object[] Case(string sql, params string[] expectedColumns) =>
        [sql, expectedColumns];
}
