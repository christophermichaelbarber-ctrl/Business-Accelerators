CREATE TABLE [dbo].[layout] (
    [Income_Statement_Key]          INT            NULL,
    [Line_name]                     NVARCHAR (MAX) NULL,
    [Subtotal_From]                 INT            NULL,
    [Subtotal_To]                   INT            NULL,
    [Divide_Numerator]              INT            NULL,
    [Divide_Denominator]            INT            NULL,
    [Show_Perc]                     INT            NULL,
    [Format_String_Default]         NVARCHAR (MAX) NULL,
    [Format_String_Variance_Abs]    NVARCHAR (MAX) NULL,
    [Format_String_Variance_Per]    NVARCHAR (MAX) NULL,
    [Formatting_P_L_Hex_Text]       NVARCHAR (MAX) NULL,
    [Formatting_P_L_Hex_Background] NVARCHAR (MAX) NULL,
    [Show_Values]                   INT            NULL,
    [Calculation_type]              NVARCHAR (MAX) NULL
);


GO

