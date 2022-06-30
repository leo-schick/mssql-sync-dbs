CREATE PROCEDURE UpdateDataSetSchemaAXInfo
    @DataSetId int
AS
-- =============================================
-- Author:		Leonhard Schick
-- Create date: 12/4/2019
-- Description:	Updates schema information based
--              on the knowledge that the source
--              system is Dynamics AX (2009/2012)
-- =============================================
BEGIN
    SET NOCOUNT ON;

    UPDATE [DataTableField]
    SET [IsModifiedDateTimeField]=1
    FROM [dbo].[DataTableField]
    INNER JOIN dbo.DataTable ON DataTable.Id = [DataTableField].DataTableId
    WHERE DataTable.DataSetId = @DataSetId
    AND [DataTableField].SourceName='MODIFIEDDATETIME';

    UPDATE [DataTableField]
    SET [IsCreatedDateTimeField]=1
    FROM [dbo].[DataTableField]
    INNER JOIN dbo.DataTable ON DataTable.Id = [DataTableField].DataTableId
    WHERE DataTable.DataSetId = @DataSetId
    AND [DataTableField].SourceName='CREATEDDATETIME';

    UPDATE [DataTableField]
    SET [IsIncrementalIdField]=1
    FROM [dbo].[DataTableField]
    INNER JOIN dbo.DataTable ON DataTable.Id = [DataTableField].DataTableId
    WHERE DataTable.DataSetId = @DataSetId
    AND [DataTableField].SourceName='RECID';

    UPDATE DataTable
    SET TransferMethod=3
    FROM dbo.DataTable
    WHERE DataTable.DataSetId = @DataSetId
    AND EXISTS (
        -- modified field exists
        SELECT 1
        FROM dbo.DataTableField
        WHERE DataTableField.DataTableId = DataTable.Id
        AND DataTableField.IsModifiedDateTimeField = 1
    ) AND (
        EXISTS (
            -- key field exists
            SELECT 1
            FROM dbo.DataTableField
            WHERE DataTableField.DataTableId = DataTable.Id
            AND DataTableField.KeyIndex IS NOT NULL
            AND DataTableField.KeyIndex >= 1
        ) OR EXISTS (
            -- incremental field exists
            SELECT TOP 1 1
            FROM dbo.DataTableField
            WHERE DataTableField.DataTableId = DataTable.Id
            AND DataTableField.IsIncrementalIdField = 1
        )
    )
END
GO
