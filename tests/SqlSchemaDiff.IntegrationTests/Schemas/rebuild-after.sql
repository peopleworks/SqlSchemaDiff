-- ===========================================================================
-- The shape the same table has to end up with: rebuild-before.sql with an
-- IDENTITY on [WidgetId] and a NOT NULL [Colour] that did not exist before.
--
-- The identity is what makes this a rebuild — there is no ALTER TABLE that adds
-- one. [Colour] is here to prove the second half: a column the target does not
-- have gets no value from the copy, so its default has to be on the temporary
-- table from the first statement or the INSERT rejects every existing row.
-- ===========================================================================

CREATE SCHEMA inv;
GO

CREATE TABLE inv.Widget
(
    WidgetId int            IDENTITY(1, 1) NOT NULL,
    Sku      varchar(40)    NOT NULL,
    Price    decimal(12, 2) NOT NULL CONSTRAINT DF_Widget_Price DEFAULT ((0)),
    Weight   int            NULL,
    Colour   varchar(20)    NOT NULL CONSTRAINT DF_Widget_Colour DEFAULT ('grey'),
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
