CREATE TABLE [dbo].[date] (
    [FiscalDate]             DATE           NULL,
    [FiscalYear]             INT            NULL,
    [FiscalQuarter]          NVARCHAR (MAX) NULL,
    [FiscalPeriod]           NVARCHAR (MAX) NULL,
    [FiscalYearQuarter]      NVARCHAR (MAX) NULL,
    [FiscalYearPeriod]       NVARCHAR (MAX) NULL,
    [FiscalYearSort]         INT            NULL,
    [FiscalQuarterSort]      INT            NULL,
    [FiscalPeriodSort]       INT            NULL,
    [FiscalYearQuarterSort]  BIGINT         NULL,
    [FiscalYearPeriodSort]   BIGINT         NULL,
    [FiscalPeriodCP]         NVARCHAR (MAX) NULL,
    [FiscalPeriodCPSort]     INT            NULL,
    [FutureDate]             BIT            NULL,
    [FiscalYearPeriodNumber] INT            NULL,
    [FiscalPeriodNumber]     INT            NULL
);


GO

