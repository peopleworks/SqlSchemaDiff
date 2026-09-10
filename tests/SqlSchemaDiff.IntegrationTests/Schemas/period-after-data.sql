-- ===========================================================================
-- WP 1.8a. A source whose archived history ends well before now, so a restore
-- can be asked the one question that used to have no answer: what did the row
-- look like between the end of the history and the moment of the restore?
--
-- The source is itself built by the recipe under test — plain period columns,
-- rows with explicit dates, then ADD PERIOD and SYSTEM_VERSIONING = ON. That is
-- the only way to get a temporal table whose ValidFrom is a date of the test's
-- choosing: SQL Server writes that column itself once the period exists.
--
-- Four sections. SOURCE and MEMSOURCE build the two source databases; HISTORY
-- and MEMHISTORY are the history tables a restore has to create for itself,
-- because the extractor never scripts one — SQL Server owns it, so the snapshot
-- carries only the schema it lives in.
-- ===========================================================================

-- @@SOURCE@@
IF SCHEMA_ID(N'archivo') IS NULL
    EXEC(N'CREATE SCHEMA [archivo]');
GO

CREATE TABLE dbo.Contrato
(
    ContratoId int           NOT NULL CONSTRAINT PK_Contrato PRIMARY KEY CLUSTERED,
    Titular    nvarchar(80)  NOT NULL,
    Importe    decimal(19,4) NOT NULL CONSTRAINT DF_Contrato_Importe DEFAULT (0),
    Desde      datetime2(7)  NOT NULL,
    Hasta      datetime2(7)  NOT NULL
);
GO

CREATE TABLE archivo.ContratoHistoria
(
    ContratoId int           NOT NULL,
    Titular    nvarchar(80)  NOT NULL,
    Importe    decimal(19,4) NOT NULL,
    Desde      datetime2(7)  NOT NULL,
    Hasta      datetime2(7)  NOT NULL
);
GO

-- The current row keeps the ValidFrom it was given; its ValidTo is the maximum
-- for the column's scale, which is what ADD PERIOD demands of every row.
INSERT INTO dbo.Contrato (ContratoId, Titular, Importe, Desde, Hasta)
VALUES (1, N'tercero', 300, '2023-01-01T00:00:00.0000000', '9999-12-31T23:59:59.9999999');
GO

INSERT INTO archivo.ContratoHistoria (ContratoId, Titular, Importe, Desde, Hasta)
VALUES (1, N'primero',  100, '2021-01-01T00:00:00.0000000', '2022-01-01T00:00:00.0000000'),
       (1, N'segundo',  200, '2022-01-01T00:00:00.0000000', '2023-01-01T00:00:00.0000000');
GO

-- HIDDEN cannot be set before the period: SQL Server only takes it on a column
-- that is already GENERATED ALWAYS (error 13735).
ALTER TABLE dbo.Contrato ADD PERIOD FOR SYSTEM_TIME (Desde, Hasta);
GO
ALTER TABLE dbo.Contrato ALTER COLUMN Desde ADD HIDDEN;
ALTER TABLE dbo.Contrato ALTER COLUMN Hasta ADD HIDDEN;
GO
ALTER TABLE dbo.Contrato SET (SYSTEM_VERSIONING = ON (HISTORY_TABLE = archivo.ContratoHistoria));
GO

-- A second table whose period columns are not hidden, so the restore has to
-- leave them alone rather than hide them on the way back.
CREATE TABLE dbo.Poliza
(
    PolizaId int          NOT NULL CONSTRAINT PK_Poliza PRIMARY KEY CLUSTERED,
    Ramo     nvarchar(40) NOT NULL,
    Desde    datetime2(3) NOT NULL,
    Hasta    datetime2(3) NOT NULL
);
GO

CREATE TABLE archivo.PolizaHistoria
(
    PolizaId int          NOT NULL,
    Ramo     nvarchar(40) NOT NULL,
    Desde    datetime2(3) NOT NULL,
    Hasta    datetime2(3) NOT NULL
);
GO

-- Scale 3, so "the maximum datetime" is .999 and not .9999999. A renderer that
-- dropped the scale would put datetime2(7) on the target and this row would then
-- fail ADD PERIOD with error 13575.
INSERT INTO dbo.Poliza (PolizaId, Ramo, Desde, Hasta)
VALUES (1, N'vida', '2023-01-01T00:00:00.000', '9999-12-31T23:59:59.999');
GO

INSERT INTO archivo.PolizaHistoria (PolizaId, Ramo, Desde, Hasta)
VALUES (1, N'salud', '2021-01-01T00:00:00.000', '2023-01-01T00:00:00.000');
GO

ALTER TABLE dbo.Poliza ADD PERIOD FOR SYSTEM_TIME (Desde, Hasta);
GO
ALTER TABLE dbo.Poliza SET (SYSTEM_VERSIONING = ON (HISTORY_TABLE = archivo.PolizaHistoria));
GO

-- @@HISTORY@@
-- What the restore has to build for itself. A history table is never in the
-- snapshot: SQL Server owns it, so the extractor skips it and the SYSTEM_VERSIONING
-- clause would create an empty one. A restore that wants the rows back creates it
-- first, in the shape the current table has minus the period.
CREATE TABLE archivo.ContratoHistoria
(
    ContratoId int           NOT NULL,
    Titular    nvarchar(80)  NOT NULL,
    Importe    decimal(19,4) NOT NULL,
    Desde      datetime2(7)  NOT NULL,
    Hasta      datetime2(7)  NOT NULL
);
GO

CREATE TABLE archivo.PolizaHistoria
(
    PolizaId int          NOT NULL,
    Ramo     nvarchar(40) NOT NULL,
    Desde    datetime2(3) NOT NULL,
    Hasta    datetime2(3) NOT NULL
);
GO

-- @@MEMSOURCE@@
-- The combination the specification expected to be impossible. It is not: a
-- memory-optimized table takes ALTER TABLE ... ADD PERIOD on already-loaded rows,
-- ALTER COLUMN ... ADD HIDDEN, and SYSTEM_VERSIONING = ON against a populated
-- disk-based history table, exactly like a disk-based one.
CREATE TABLE dbo.MemContrato
(
    ContratoId int          NOT NULL,
    Titular    nvarchar(80) COLLATE Latin1_General_100_BIN2 NOT NULL,
    Desde      datetime2(7) NOT NULL,
    Hasta      datetime2(7) NOT NULL,
    CONSTRAINT PK_MemContrato PRIMARY KEY NONCLUSTERED HASH (ContratoId) WITH (BUCKET_COUNT = 256)
)
WITH (MEMORY_OPTIMIZED = ON, DURABILITY = SCHEMA_AND_DATA);
GO

CREATE TABLE dbo.MemContratoHistoria
(
    ContratoId int          NOT NULL,
    Titular    nvarchar(80) COLLATE Latin1_General_100_BIN2 NOT NULL,
    Desde      datetime2(7) NOT NULL,
    Hasta      datetime2(7) NOT NULL
);
GO

INSERT INTO dbo.MemContrato (ContratoId, Titular, Desde, Hasta)
VALUES (1, N'tercero', '2023-01-01T00:00:00.0000000', '9999-12-31T23:59:59.9999999');
GO

INSERT INTO dbo.MemContratoHistoria (ContratoId, Titular, Desde, Hasta)
VALUES (1, N'primero', '2021-01-01T00:00:00.0000000', '2023-01-01T00:00:00.0000000');
GO

ALTER TABLE dbo.MemContrato ADD PERIOD FOR SYSTEM_TIME (Desde, Hasta);
GO
ALTER TABLE dbo.MemContrato SET (SYSTEM_VERSIONING = ON (HISTORY_TABLE = dbo.MemContratoHistoria));
GO

-- @@MEMHISTORY@@
CREATE TABLE dbo.MemContratoHistoria
(
    ContratoId int          NOT NULL,
    Titular    nvarchar(80) COLLATE Latin1_General_100_BIN2 NOT NULL,
    Desde      datetime2(7) NOT NULL,
    Hasta      datetime2(7) NOT NULL
);
GO
