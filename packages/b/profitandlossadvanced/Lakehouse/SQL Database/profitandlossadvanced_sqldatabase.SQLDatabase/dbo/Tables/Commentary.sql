CREATE TABLE [dbo].[Commentary] (
    [CommentaryID]        INT              IDENTITY (1, 1) NOT NULL,
    [Description]         NVARCHAR (400)   NOT NULL,
    [Level]               NVARCHAR (400)   NULL,
    [Date]                DATETIME         NULL,
    [Time_Horizon_Key]    INT              NULL,
    [Fx_Assumptions_Key]  INT              NULL,
    [Legal_Entity_Key]    INT              NULL,
    [User Name]           NVARCHAR (400)   NULL,
    [User Principle Name] NVARCHAR (400)   NULL,
    [rowguid]             UNIQUEIDENTIFIER CONSTRAINT [DF_Commentary_rowguid] DEFAULT (newid()) NOT NULL,
    [ModifiedDate]        DATETIME         CONSTRAINT [DF_Commentary_ModifiedDate] DEFAULT (getdate()) NOT NULL,
    CONSTRAINT [PK_Commentary_CommentaryID] PRIMARY KEY CLUSTERED ([CommentaryID] ASC),
    CONSTRAINT [AK_Commentary_rowguid] UNIQUE NONCLUSTERED ([rowguid] ASC)
);


GO

