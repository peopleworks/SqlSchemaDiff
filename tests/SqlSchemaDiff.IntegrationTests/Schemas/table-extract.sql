-- ===========================================================================
-- WP 1.8b: the shapes a per-table extract has to get right, and in particular
-- everything a *history* table does and does not carry.
--
-- The parent tables here deliberately pile on the column kinds whose history
-- copy is not obvious: an identity, a computed column both persisted and not,
-- a ROWGUIDCOL, a SPARSE column, a rowversion, an alias type, an nvarchar(max),
-- a non-default collation, defaults, a check and a foreign key. What SQL Server
-- keeps and what it drops on the way into the history table is measured against
-- sys.columns by the tests, not assumed.
-- ===========================================================================

IF SCHEMA_ID(N'hist') IS NULL
    EXEC(N'CREATE SCHEMA [hist]');
GO

-- An alias type, to prove a history table's column keeps the *user* type and not
-- the system type underneath it: a restore of history needs the type to exist.
CREATE TYPE dbo.Codigo FROM nvarchar(20) NOT NULL;
GO

CREATE TABLE dbo.Cliente
(
    ClienteId int          NOT NULL CONSTRAINT PK_Cliente PRIMARY KEY CLUSTERED,
    Nombre    nvarchar(60) NOT NULL
);
GO

-- The rich one. Its history table is created by SQL Server from the
-- SYSTEM_VERSIONING clause, so its shape is the server's decision, not ours.
CREATE TABLE dbo.Pedido
(
    PedidoId  int            IDENTITY(10, 5) NOT NULL CONSTRAINT PK_Pedido PRIMARY KEY CLUSTERED,
    ClienteId int            NOT NULL,
    Sku       dbo.Codigo     NOT NULL,
    Nota      nvarchar(max)  NULL,
    Grito     AS UPPER([Sku]),
    GritoFijo AS LOWER([Sku]) PERSISTED,
    Rastro    uniqueidentifier ROWGUIDCOL NOT NULL CONSTRAINT DF_Pedido_Rastro DEFAULT (NEWID()),
    Total     decimal(19, 4) NOT NULL CONSTRAINT DF_Pedido_Total DEFAULT (0),
    Etiqueta  varchar(50)    COLLATE Latin1_General_CS_AS NULL,
    Flojo     int            SPARSE NULL,
    Version   rowversion     NOT NULL,
    Desde     datetime2(7)   GENERATED ALWAYS AS ROW START HIDDEN NOT NULL,
    Hasta     datetime2(7)   GENERATED ALWAYS AS ROW END   HIDDEN NOT NULL,
    PERIOD FOR SYSTEM_TIME (Desde, Hasta),
    CONSTRAINT CK_Pedido_Total CHECK (Total >= 0),
    CONSTRAINT FK_Pedido_Cliente FOREIGN KEY (ClienteId) REFERENCES dbo.Cliente (ClienteId)
)
WITH (SYSTEM_VERSIONING = ON (HISTORY_TABLE = hist.PedidoHistoria));
GO

CREATE NONCLUSTERED INDEX IX_Pedido_Sku ON dbo.Pedido (Sku DESC) INCLUDE (Total, Etiqueta);
GO

CREATE UNIQUE NONCLUSTERED INDEX UX_Pedido_Rastro ON dbo.Pedido (Rastro) WHERE Etiqueta IS NOT NULL;
GO

-- A history table the *user* built, with a clustered columnstore index of its
-- own — which is what SQL Server documents for large history and what a schema
-- with no rows would never show.
CREATE TABLE dbo.Evento
(
    EventoId int          NOT NULL CONSTRAINT PK_Evento PRIMARY KEY CLUSTERED,
    Titulo   nvarchar(80) NULL,
    Abre     datetime2(7) GENERATED ALWAYS AS ROW START NOT NULL,
    Cierra   datetime2(7) GENERATED ALWAYS AS ROW END   NOT NULL,
    PERIOD FOR SYSTEM_TIME (Abre, Cierra)
);
GO

CREATE TABLE hist.EventoHistoria
(
    EventoId int          NOT NULL,
    Titulo   nvarchar(80) NULL,
    Abre     datetime2(7) NOT NULL,
    Cierra   datetime2(7) NOT NULL
);
GO

CREATE CLUSTERED COLUMNSTORE INDEX CCI_EventoHistoria ON hist.EventoHistoria;
GO

ALTER TABLE dbo.Evento
    SET (SYSTEM_VERSIONING = ON (HISTORY_TABLE = hist.EventoHistoria, DATA_CONSISTENCY_CHECK = ON));
GO

-- An ordinary compressed heap: no clustered index, so the compression is the
-- table's own and nothing about it is temporal.
CREATE TABLE dbo.Plano
(
    PlanoId int          NOT NULL,
    Texto   nvarchar(40) NULL
)
WITH (DATA_COMPRESSION = PAGE);
GO

-- Rows, and then changes to them, so every history table has real data in it and
-- the tests are reading a populated shape rather than an empty one.
INSERT INTO dbo.Cliente (ClienteId, Nombre) VALUES (1, N'Ada'), (2, N'Grace');
GO

INSERT INTO dbo.Pedido (ClienteId, Sku, Nota, Total, Etiqueta, Flojo)
VALUES (1, N'A-1', N'primero', 10, 'uno', 7),
       (2, N'B-2', NULL, 20, NULL, NULL);
GO

UPDATE dbo.Pedido SET Total = Total + 1;
GO

INSERT INTO dbo.Evento (EventoId, Titulo) VALUES (1, N'Arranque');
GO

UPDATE dbo.Evento SET Titulo = N'Arranque (revisado)' WHERE EventoId = 1;
GO

INSERT INTO dbo.Plano (PlanoId, Texto) VALUES (1, N'llano');
GO
