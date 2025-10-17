CREATE TABLE [dbo].[ledger] (
    [Date]                      DATETIME2 (6) NULL,
    [AccountKey]                BIGINT        NULL,
    [CreditlessdebitatGlobalFx] FLOAT (53)    NULL,
    [CreditlessdebitatPYFx]     FLOAT (53)    NULL,
    [CreditlessdebitatFcstFx]   FLOAT (53)    NULL,
    [CreditlessdebitatPlanFx]   FLOAT (53)    NULL,
    [DocumentID]                BIGINT        NULL,
    [CurrencyKey]               BIGINT        NULL,
    [LegalEntityKey]            BIGINT        NULL
);


GO

