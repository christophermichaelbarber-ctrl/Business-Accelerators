CREATE TABLE [dbo].[CommentaryDescription] (
    [CommentaryDescriptionID] INT              IDENTITY (1, 1) NOT NULL,
    [Description]             NVARCHAR (400)   NOT NULL,
    [rowguid]                 UNIQUEIDENTIFIER CONSTRAINT [DF_CommentaryDescription_rowguid] DEFAULT (newid()) NOT NULL,
    [ModifiedDate]            DATETIME         CONSTRAINT [DF_CommentaryDescription_ModifiedDate] DEFAULT (getdate()) NOT NULL,
    CONSTRAINT [PK_CommentaryDescription_CommentaryDescriptionID] PRIMARY KEY CLUSTERED ([CommentaryDescriptionID] ASC),
    CONSTRAINT [AK_CommentaryDescription_rowguid] UNIQUE NONCLUSTERED ([rowguid] ASC)
);


GO

