-- ===========================================================================
-- The shape a table has before a rebuild, with rows in it.
--
-- Paired with rebuild-after.sql, which is the same schema with an identity on
-- the key and one extra NOT NULL column: the change ALTER TABLE has no syntax
-- for, and so the one that forces the engine to build the table again and carry
-- the rows across.
--
-- Everything here is a thing the rebuild has to preserve or work around: rows, a
-- named default whose name the temporary table cannot borrow, a computed column
-- and a rowversion that no INSERT is allowed to write, a unique constraint, a
-- check, an index, an inbound foreign key from another table, and a trigger that
-- DROP TABLE would otherwise take away without saying so.
-- ===========================================================================

CREATE SCHEMA inv;
GO

CREATE TABLE inv.Widget
(
    WidgetId int            NOT NULL,
    Sku      varchar(40)    NOT NULL,
    Price    decimal(12, 2) NOT NULL CONSTRAINT DF_Widget_Price DEFAULT ((0)),
    Weight   int            NULL,
    Doubled  AS (Price * 2) PERSISTED,
    Stamp    rowversion     NOT NULL,
    CONSTRAINT PK_Widget PRIMARY KEY CLUSTERED (WidgetId),
    CONSTRAINT UQ_Widget_Sku UNIQUE NONCLUSTERED (Sku),
    CONSTRAINT CK_Widget_Price CHECK (Price >= 0)
);
GO

CREATE NONCLUSTERED INDEX IX_Widget_Sku ON inv.Widget (Sku) INCLUDE (Price);
GO

CREATE TABLE inv.WidgetLog
(
    LogId    int          IDENTITY(1, 1) NOT NULL,
    WidgetId int          NOT NULL,
    Note     nvarchar(80) NULL,
    CONSTRAINT PK_WidgetLog PRIMARY KEY CLUSTERED (LogId),
    CONSTRAINT FK_WidgetLog_Widget FOREIGN KEY (WidgetId) REFERENCES inv.Widget (WidgetId)
);
GO

CREATE TRIGGER inv.trWidgetTouch
ON inv.Widget
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO inv.WidgetLog (WidgetId, Note)
    SELECT i.WidgetId, N'updated'
    FROM inserted AS i;
END;
GO

INSERT INTO inv.Widget (WidgetId, Sku, Price, Weight)
VALUES (1, 'W-001', 10.00, 5),
       (2, 'W-002', 20.50, NULL),
       (3, 'W-003', 30.25, 7);
GO

INSERT INTO inv.WidgetLog (WidgetId, Note) VALUES (1, N'created');
GO
