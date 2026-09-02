-- ===========================================================================
-- Everything SqlServerSchemaExtractor captures today, in one database.
--
-- This is the fixture for the live tests: extract it, script it back out, apply
-- it to an empty database and the two have to compare equal. Each construct here
-- exists because it has its own branch in the extractor or the renderer, so if
-- you add one to the engine, add it here too.
--
-- Deliberately NOT here: temporal and memory-optimized tables. The extractor
-- captures their flags but does not script them yet, and reports them as
-- notices; they are covered by their own test in a throwaway database.
-- ===========================================================================

CREATE SCHEMA sales;
GO

-- A schema owned by a principal other than dbo. The owner is captured, and the
-- scripts name it only when the target has the principal, so a restore into an
-- empty database still gets the schema and the drift report says who owns it.
CREATE USER ops_owner WITHOUT LOGIN;
GO

CREATE SCHEMA ops AUTHORIZATION ops_owner;
GO

-- Two alias types on purpose: a string one, which sys.types reports with a
-- collation, and a numeric one, which reports NULL. The renderer has to leave the
-- COLLATE clause off columns typed with either, and only the first one proves it.
CREATE TYPE sales.AccountCode FROM varchar(20) NOT NULL;
GO

CREATE TYPE sales.Amount FROM decimal(19, 4) NULL;
GO

-- ---------------------------------------------------------------- sequences

-- Referenced by a default on ops.Ticket, so the sequence has to exist before the
-- table. The catalog records no such edge: it comes from reading the default.
CREATE SEQUENCE sales.InvoiceNumber AS int
    START WITH 1000 INCREMENT BY 1 MINVALUE 1 NO MAXVALUE NO CYCLE CACHE 20;
GO

-- -------------------------------------------------------------- table types

-- Constraints inline and unnamed: a table type cannot be altered, so its whole
-- definition is one statement and any change is a drop and create.
CREATE TYPE sales.InvoiceLineList AS TABLE
(
    Sku       varchar(60)    NOT NULL PRIMARY KEY,
    Qty       int            NOT NULL CHECK (Qty > 0),
    UnitPrice decimal(19, 4) NULL
);
GO

-- ------------------------------------------------------------------- tables

-- sales.Terms is referenced by sales.Invoice, whose name sorts EARLIER. Anything
-- that emits tables in alphabetical order and puts foreign keys inline with the
-- CREATE TABLE fails right here.
CREATE TABLE sales.Terms
(
    TermsId  int               IDENTITY(500, 7) NOT NULL,
    Code     sales.AccountCode NOT NULL,
    NetDays  smallint          NOT NULL CONSTRAINT DF_Terms_NetDays DEFAULT ((30)),
    IsActive bit               NOT NULL DEFAULT ((1)),
    CONSTRAINT PK_Terms PRIMARY KEY CLUSTERED (TermsId),
    CONSTRAINT UQ_Terms_Code UNIQUE NONCLUSTERED (Code)
);
GO

CREATE TABLE sales.Customer
(
    CustomerId  int              IDENTITY(1000, 5) NOT NULL,
    ExternalId  uniqueidentifier ROWGUIDCOL NOT NULL CONSTRAINT DF_Customer_ExternalId DEFAULT (NEWID()),
    FirstName   nvarchar(60)     NOT NULL,
    LastName    nvarchar(60)     NOT NULL,
    FullName    AS ([FirstName] + N' ' + [LastName]),
    SortKey     varchar(120)     COLLATE Latin1_General_BIN2 NULL,
    Email       varchar(256)     NULL,
    CreditLimit decimal(12, 2)   NOT NULL CONSTRAINT DF_Customer_CreditLimit DEFAULT ((0)),
    Rating      numeric(4, 1)    NULL,
    Balance     money            NOT NULL DEFAULT ((0)),
    Notes       varchar(max)     NULL,
    Photo       varbinary(max)   NULL,
    IsActive    bit              NOT NULL CONSTRAINT DF_Customer_IsActive DEFAULT ((1)),
    CreatedAt   datetime2(3)     NOT NULL CONSTRAINT DF_Customer_CreatedAt DEFAULT (SYSUTCDATETIME()),
    CONSTRAINT PK_Customer PRIMARY KEY CLUSTERED (CustomerId),
    CONSTRAINT CK_Customer_CreditLimit CHECK (CreditLimit >= 0)
);
GO

CREATE TABLE sales.Invoice
(
    InvoiceId  bigint         IDENTITY(1, 1) NOT NULL,
    CustomerId int            NOT NULL,
    TermsId    int            NULL,
    Sku        varchar(60)    NOT NULL,
    Qty        int            NOT NULL CONSTRAINT DF_Invoice_Qty DEFAULT ((1)),
    UnitPrice  sales.Amount   NULL,
    LineTotal  AS ([Qty] * [UnitPrice]) PERSISTED,
    IssuedAt   datetime2(3)   NOT NULL CONSTRAINT DF_Invoice_IssuedAt DEFAULT (SYSUTCDATETIME()),
    Attachment varbinary(max) NULL,
    Memo       nvarchar(400)  NULL,
    -- A nonclustered primary key with the clustered index somewhere else: the
    -- renderer has to carry CLUSTERED/NONCLUSTERED per constraint, not assume it.
    CONSTRAINT PK_Invoice PRIMARY KEY NONCLUSTERED (InvoiceId),
    CONSTRAINT UQ_Invoice_Customer_Sku UNIQUE CLUSTERED (CustomerId, Sku),
    CONSTRAINT CK_Invoice_Qty CHECK (Qty > 0),
    CONSTRAINT FK_Invoice_Customer FOREIGN KEY (CustomerId)
        REFERENCES sales.Customer (CustomerId) ON DELETE CASCADE,
    -- ON UPDATE SET NULL needs a nullable child column.
    CONSTRAINT FK_Invoice_Terms FOREIGN KEY (TermsId)
        REFERENCES sales.Terms (TermsId) ON UPDATE SET NULL
);
GO

CREATE TABLE ops.AuditEntry
(
    AuditId    int          IDENTITY(1, 1) NOT NULL,
    InvoiceId  bigint       NULL,
    CustomerId int          NULL,
    EventKind  nvarchar(40) NOT NULL,
    Source     varchar(40)  NOT NULL CONSTRAINT DF_AuditEntry_Source DEFAULT ('system'),
    Payload    varchar(max) NULL,
    OccurredAt datetime2(3) NOT NULL CONSTRAINT DF_AuditEntry_OccurredAt DEFAULT (SYSUTCDATETIME()),
    CONSTRAINT PK_AuditEntry PRIMARY KEY CLUSTERED (AuditId)
);
GO

-- Defaults to the sequence, so the create order has to put sales.InvoiceNumber
-- ahead of this table.
CREATE TABLE ops.Ticket
(
    TicketId int           NOT NULL CONSTRAINT DF_Ticket_TicketId DEFAULT (NEXT VALUE FOR sales.InvoiceNumber),
    Title    nvarchar(100) NOT NULL,
    CONSTRAINT PK_Ticket PRIMARY KEY CLUSTERED (TicketId)
);
GO

-- An untrusted foreign key: WITH NOCHECK leaves is_not_trusted = 1 but the key
-- stays enabled.
ALTER TABLE ops.AuditEntry WITH NOCHECK
    ADD CONSTRAINT FK_AuditEntry_Invoice FOREIGN KEY (InvoiceId) REFERENCES sales.Invoice (InvoiceId);
GO

-- A disabled foreign key: created trusted, then switched off. is_disabled = 1 and
-- is_not_trusted = 1, and the renderer has to emit both the ADD and the NOCHECK.
ALTER TABLE ops.AuditEntry WITH CHECK
    ADD CONSTRAINT FK_AuditEntry_Customer FOREIGN KEY (CustomerId) REFERENCES sales.Customer (CustomerId);
GO

ALTER TABLE ops.AuditEntry NOCHECK CONSTRAINT FK_AuditEntry_Customer;
GO

ALTER TABLE ops.AuditEntry WITH CHECK
    ADD CONSTRAINT CK_AuditEntry_EventKind CHECK (LEN(EventKind) > 0);
GO

-- A disabled check constraint.
ALTER TABLE ops.AuditEntry NOCHECK CONSTRAINT CK_AuditEntry_EventKind;
GO

-- ------------------------------------------------------------------ indexes

-- Unique, filtered, with included columns: three renderer branches in one index.
CREATE UNIQUE NONCLUSTERED INDEX UX_Customer_Email
    ON sales.Customer (Email)
    INCLUDE (FirstName, LastName)
    WHERE Email IS NOT NULL;
GO

-- A descending key column, and an ascending one after it, so the order of the key
-- list matters as well as its direction.
CREATE NONCLUSTERED INDEX IX_Invoice_IssuedAt
    ON sales.Invoice (IssuedAt DESC, Sku ASC);
GO

-- Plain, single column. The ALTER test widens [Source] underneath it, which forces
-- the differ to drop this index and put it back around the ALTER COLUMN.
CREATE NONCLUSTERED INDEX IX_AuditEntry_Source
    ON ops.AuditEntry (Source);
GO

-- ---------------------------------------------------------------- functions

-- Scalar.
CREATE FUNCTION sales.fnLineTotal(@qty int, @unitPrice decimal(19, 4))
RETURNS decimal(19, 4)
AS
BEGIN
    RETURN ISNULL(@qty, 0) * ISNULL(@unitPrice, 0);
END;
GO

-- Inline table-valued.
CREATE FUNCTION sales.fnCustomersByRating(@minRating numeric(4, 1))
RETURNS TABLE
AS
RETURN
(
    SELECT c.CustomerId, c.FullName, c.Rating
    FROM sales.Customer AS c
    WHERE c.Rating >= @minRating
);
GO

-- Multi-statement table-valued.
CREATE FUNCTION ops.fnRecentAudit(@take int)
RETURNS @result TABLE
(
    AuditId    int          NOT NULL,
    EventKind  nvarchar(40) NOT NULL,
    OccurredAt datetime2(3) NOT NULL
)
AS
BEGIN
    INSERT INTO @result (AuditId, EventKind, OccurredAt)
    SELECT TOP (@take) a.AuditId, a.EventKind, a.OccurredAt
    FROM ops.AuditEntry AS a
    ORDER BY a.OccurredAt DESC;

    RETURN;
END;
GO

-- -------------------------------------------------------------------- views

-- Schema-bound: needs two-part names and QUOTED_IDENTIFIER ON at creation time.
CREATE VIEW sales.vInvoiceTotals
WITH SCHEMABINDING
AS
    SELECT i.InvoiceId, i.CustomerId, i.Sku, i.Qty, i.LineTotal
    FROM sales.Invoice AS i;
GO

-- A view on a view: the create order has to come from the dependency graph, not
-- from the object type alone.
CREATE VIEW sales.vInvoiceTotalsByCustomer
AS
    SELECT v.CustomerId, COUNT_BIG(*) AS InvoiceCount, SUM(v.LineTotal) AS Total
    FROM sales.vInvoiceTotals AS v
    GROUP BY v.CustomerId;
GO

-- A view that calls a scalar function, so a view can depend on a function too.
CREATE VIEW sales.vInvoiceComputed
AS
    SELECT i.InvoiceId, sales.fnLineTotal(i.Qty, i.UnitPrice) AS ComputedTotal
    FROM sales.Invoice AS i;
GO

-- --------------------------------------------------------------- procedures

-- Created with QUOTED_IDENTIFIER OFF on purpose: sys.sql_modules records the
-- session setting alongside the text, and the scripts the engine generates always
-- run with the option ON. The body avoids double-quoted literals so it stays
-- valid either way — the point is that the round trip survives the difference.
SET QUOTED_IDENTIFIER OFF;
GO

CREATE PROCEDURE ops.uspTouchAudit
    @eventKind nvarchar(40)
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO ops.AuditEntry (EventKind)
    VALUES (@eventKind);
END;
GO

SET QUOTED_IDENTIFIER ON;
GO

CREATE PROCEDURE sales.uspCustomerSummary
    @customerId int
AS
BEGIN
    SET NOCOUNT ON;

    SELECT c.CustomerId, c.FullName, t.Total
    FROM sales.Customer AS c
    LEFT JOIN sales.vInvoiceTotalsByCustomer AS t ON t.CustomerId = c.CustomerId
    WHERE c.CustomerId = @customerId;
END;
GO

-- Takes the table type as a parameter: the type has to exist before the procedure
-- and cannot be dropped while the procedure does.
CREATE PROCEDURE sales.uspAddInvoiceLines
    @lines sales.InvoiceLineList READONLY
AS
BEGIN
    SET NOCOUNT ON;

    SELECT COUNT(*) AS LineCount FROM @lines;
END;
GO

-- ----------------------------------------------------------------- triggers

-- An enabled AFTER trigger that writes into another schema.
CREATE TRIGGER sales.trCustomerTouch
ON sales.Customer
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO ops.AuditEntry (EventKind, CustomerId)
    SELECT N'customer.update', i.CustomerId
    FROM inserted AS i;
END;
GO

-- An INSTEAD OF trigger created and then disabled. The state is not part of the
-- module text, so the round trip has to carry it separately and re-apply it
-- after every create or alter.
CREATE TRIGGER ops.trAuditEntryBlockDelete
ON ops.AuditEntry
INSTEAD OF DELETE
AS
BEGIN
    SET NOCOUNT ON;

    RAISERROR (N'audit rows are immutable', 16, 1);
END;
GO

DISABLE TRIGGER ops.trAuditEntryBlockDelete ON ops.AuditEntry;
GO
