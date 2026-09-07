-- ===========================================================================
-- The two table shapes 1.6 captured and refused to script. Both need syntax
-- that only exists inside CREATE TABLE, and a system-versioned table also
-- needs the restore to hold off turning versioning on until its rows are in.
--
-- Split into two sections on purpose: the memory-optimized half needs a
-- filegroup CONTAINS MEMORY_OPTIMIZED_DATA, which not every server will take,
-- and the temporal half has to run either way.
-- ===========================================================================

-- @@TEMPORAL@@
IF SCHEMA_ID(N'history') IS NULL
    EXEC(N'CREATE SCHEMA [history]');
GO

-- A history table in a schema of its own: SQL Server creates it from the
-- SYSTEM_VERSIONING clause, but the schema has to be there first.
CREATE TABLE dbo.Employee
(
    EmployeeId  int           NOT NULL CONSTRAINT PK_Employee PRIMARY KEY CLUSTERED,
    FullName    nvarchar(80)  NOT NULL,
    Department  nvarchar(40)  NULL,
    Salary      decimal(19,4) NOT NULL CONSTRAINT DF_Employee_Salary DEFAULT (0),
    ValidFrom   datetime2(7)  GENERATED ALWAYS AS ROW START HIDDEN NOT NULL,
    ValidTo     datetime2(7)  GENERATED ALWAYS AS ROW END   HIDDEN NOT NULL,
    PERIOD FOR SYSTEM_TIME (ValidFrom, ValidTo)
)
WITH (SYSTEM_VERSIONING = ON (HISTORY_TABLE = history.EmployeeEntryHistory));
GO

CREATE NONCLUSTERED INDEX IX_Employee_Department ON dbo.Employee (Department) INCLUDE (FullName);
GO

ALTER TABLE dbo.Employee WITH CHECK ADD CONSTRAINT CK_Employee_Salary CHECK (Salary >= 0);
GO

-- Rows, so the round trip has something the restore shape has to load before it
-- can turn versioning on.
INSERT INTO dbo.Employee (EmployeeId, FullName, Department, Salary)
VALUES (1, N'Ada', N'Engineering', 100), (2, N'Grace', N'Engineering', 120);
GO

-- A period without system versioning: legal, and a different code path.
CREATE TABLE dbo.Assignment
(
    AssignmentId int          NOT NULL CONSTRAINT PK_Assignment PRIMARY KEY CLUSTERED,
    Note         nvarchar(60) NULL,
    StartedAt    datetime2(7) GENERATED ALWAYS AS ROW START NOT NULL,
    EndedAt      datetime2(7) GENERATED ALWAYS AS ROW END   NOT NULL,
    PERIOD FOR SYSTEM_TIME (StartedAt, EndedAt)
);
GO

-- @@MEMORY@@
-- Every index a memory-optimized table has lives inside its CREATE TABLE: a hash
-- primary key with a bucket count, a hash index and a plain nonclustered one.
-- The key columns need a BIN2 collation, which is a property of the data, not of
-- the renderer.
CREATE TABLE dbo.MemOrder
(
    OrderId int            NOT NULL,
    Code    nvarchar(40)   COLLATE Latin1_General_100_BIN2 NOT NULL,
    Total   decimal(19, 4) NOT NULL,
    Note    nvarchar(100)  NULL,
    CONSTRAINT PK_MemOrder PRIMARY KEY NONCLUSTERED HASH (OrderId) WITH (BUCKET_COUNT = 1024),
    INDEX IX_MemOrder_Code HASH (Code) WITH (BUCKET_COUNT = 512),
    INDEX IX_MemOrder_Total NONCLUSTERED (Total DESC)
)
WITH (MEMORY_OPTIMIZED = ON, DURABILITY = SCHEMA_AND_DATA);
GO

CREATE TABLE dbo.MemScratch
(
    ScratchId int          NOT NULL,
    Label     nvarchar(30) COLLATE Latin1_General_100_BIN2 NOT NULL,
    CONSTRAINT PK_MemScratch PRIMARY KEY NONCLUSTERED (ScratchId)
)
WITH (MEMORY_OPTIMIZED = ON, DURABILITY = SCHEMA_ONLY);
GO
