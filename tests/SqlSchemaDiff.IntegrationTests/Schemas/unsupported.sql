-- ===========================================================================
-- Constructs at the edge of what the extractor models. A columnstore index is
-- captured since 1.6 and must come back typed as one; a system-versioned table
-- is captured with its flags but not scripted, so it has to come back as a
-- notice rather than an exception or a silent omission.
--
-- Split into two batches on purpose: the test runs them one at a time and skips
-- the matching assertion when an edition refuses the feature.
-- ===========================================================================

-- @@COLUMNSTORE@@
CREATE TABLE dbo.Fact
(
    FactId int            NOT NULL CONSTRAINT PK_Fact PRIMARY KEY CLUSTERED,
    Amount decimal(19, 4) NOT NULL,
    Bucket int            NOT NULL
);
GO

CREATE NONCLUSTERED COLUMNSTORE INDEX CSX_Fact_Amount ON dbo.Fact (Amount, Bucket);
GO

-- @@TEMPORAL@@
CREATE TABLE dbo.Employee
(
    EmployeeId int          NOT NULL CONSTRAINT PK_Employee PRIMARY KEY CLUSTERED,
    FullName   nvarchar(80) NOT NULL,
    ValidFrom  datetime2(7) GENERATED ALWAYS AS ROW START NOT NULL,
    ValidTo    datetime2(7) GENERATED ALWAYS AS ROW END NOT NULL,
    PERIOD FOR SYSTEM_TIME (ValidFrom, ValidTo)
)
WITH (SYSTEM_VERSIONING = ON (HISTORY_TABLE = dbo.EmployeeHistory));
GO
