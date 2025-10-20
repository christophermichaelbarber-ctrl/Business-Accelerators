CREATE TABLE [dbo].[UdfInvocationLog] (
    [LogID]           INT             IDENTITY (1, 1) NOT NULL,
    [UdfName]         [sysname]       NOT NULL,
    [CallerPrincipal] NVARCHAR (256)  NULL,
    [DbName]          [sysname]       NULL,
    [CreatedAtUtc]    DATETIME2 (3)   DEFAULT (sysutcdatetime()) NOT NULL,
    [InputSummary]    NVARCHAR (400)  NULL,
    [Succeeded]       BIT             NOT NULL,
    [InsertedID]      INT             NULL,
    [ErrorType]       NVARCHAR (200)  NULL,
    [ErrorMessage]    NVARCHAR (2000) NULL,
    [ErrorTrace]      NVARCHAR (MAX)  NULL,
    PRIMARY KEY CLUSTERED ([LogID] ASC)
);


GO

