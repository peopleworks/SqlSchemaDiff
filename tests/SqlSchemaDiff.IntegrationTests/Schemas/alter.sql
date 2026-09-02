-- ===========================================================================
-- The second half of the round trip: a plausible sprint's worth of schema drift
-- applied on top of full.sql. Every statement here maps to one branch of
-- TableDiffer or SchemaDiffer that has to produce an incremental change rather
-- than a table rebuild.
-- ===========================================================================

-- A new column, NOT NULL with a default so it can land on a populated table.
ALTER TABLE sales.Customer
    ADD LoyaltyPoints int NOT NULL CONSTRAINT DF_Customer_LoyaltyPoints DEFAULT ((0));
GO

-- Nullability change. [Rating] carries no index, so this is the plain case.
ALTER TABLE sales.Customer ALTER COLUMN Rating numeric(4, 1) NOT NULL;
GO

-- A widening on an indexed column. SQL Server refuses ALTER COLUMN while an index
-- references it, so the index comes down and goes back up — which is exactly what
-- the differ has to work out for itself on the target.
DROP INDEX IX_AuditEntry_Source ON ops.AuditEntry;
GO

ALTER TABLE ops.AuditEntry ALTER COLUMN Source varchar(80) NOT NULL;
GO

CREATE NONCLUSTERED INDEX IX_AuditEntry_Source ON ops.AuditEntry (Source);
GO

-- A brand new index.
CREATE NONCLUSTERED INDEX IX_Invoice_Sku ON sales.Invoice (Sku) INCLUDE (Qty);
GO

-- A dropped check constraint. This one is the drop-side probe: it must survive on
-- the target with --include-drops off, and disappear with it on.
ALTER TABLE sales.Customer DROP CONSTRAINT CK_Customer_CreditLimit;
GO

-- A new table with a foreign key into an existing one.
CREATE TABLE sales.Payment
(
    PaymentId int          IDENTITY(1, 1) NOT NULL,
    InvoiceId bigint       NOT NULL,
    Paid      sales.Amount NULL,
    PaidAt    datetime2(3) NOT NULL CONSTRAINT DF_Payment_PaidAt DEFAULT (SYSUTCDATETIME()),
    CONSTRAINT PK_Payment PRIMARY KEY CLUSTERED (PaymentId),
    CONSTRAINT FK_Payment_Invoice FOREIGN KEY (InvoiceId) REFERENCES sales.Invoice (InvoiceId)
);
GO

-- A changed view. CREATE OR ALTER on purpose: SQL Server blanks out the OR ALTER
-- when it stores the text, so both sides end up with the same definition and the
-- comparison is about the body, not about how it was deployed.
CREATE OR ALTER VIEW sales.vInvoiceComputed
AS
    SELECT i.InvoiceId, i.CustomerId, sales.fnLineTotal(i.Qty, i.UnitPrice) AS ComputedTotal
    FROM sales.Invoice AS i;
GO

-- A changed procedure body, with a new optional parameter.
CREATE OR ALTER PROCEDURE sales.uspCustomerSummary
    @customerId int,
    @includeInactive bit = 0
AS
BEGIN
    SET NOCOUNT ON;

    SELECT c.CustomerId, c.FullName, t.Total
    FROM sales.Customer AS c
    LEFT JOIN sales.vInvoiceTotalsByCustomer AS t ON t.CustomerId = c.CustomerId
    WHERE c.CustomerId = @customerId
      AND (@includeInactive = 1 OR c.IsActive = 1);
END;
GO
