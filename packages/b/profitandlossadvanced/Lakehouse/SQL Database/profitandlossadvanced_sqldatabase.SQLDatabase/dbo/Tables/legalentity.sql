CREATE TABLE [dbo].[legalentity] (
    [Legal_Entity_Key]          INT            NULL,
    [Legal_entity_name]         NVARCHAR (MAX) NULL,
    [Legal_Entity_Sort_Order]   INT            NULL,
    [Group]                     NVARCHAR (MAX) NULL,
    [Country]                   NVARCHAR (MAX) NULL,
    [Region]                    NVARCHAR (MAX) NULL,
    [Child]                     NVARCHAR (MAX) NULL,
    [Parent]                    NVARCHAR (MAX) NULL,
    [Path]                      NVARCHAR (MAX) NULL,
    [Legal_entity_level_1]      INT            NULL,
    [Legal_entity_level_2]      INT            NULL,
    [Legal_entity_level_3]      INT            NULL,
    [Legal_entity_level_4]      INT            NULL,
    [Legal_entity_level_5]      INT            NULL,
    [Legal_entity_level_1_name] NVARCHAR (MAX) NULL,
    [Legal_entity_level_2_name] NVARCHAR (MAX) NULL,
    [Legal_entity_level_3_name] NVARCHAR (MAX) NULL,
    [Legal_entity_level_4_name] NVARCHAR (MAX) NULL,
    [Legal_entity_level_5_name] NVARCHAR (MAX) NULL
);


GO

