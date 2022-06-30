CREATE PROCEDURE [dbo].[BuildDataSet]
    @DataSetId int,
    @ForceRecreate bit = 0
AS
-- =============================================
-- Autor:			Leonhard Schick
-- Created at:		14.09.2021
-- Last chagned at: 14.09.2021
-- =============================================
-- Description:	
--   Creates 
--
-- @DataSetId			The id of the data set - siehe table dbo.[DataSet] as well
-- @ExecutionLoadGroup  filter on a defined load group
-- @ExecutionBatchId    filter on a batch id
-- =============================================
BEGIN
    -- SET NOCOUNT ON added to prevent extra result sets from
    -- interfering with SELECT statements.
    SET NOCOUNT ON;

    DECLARE @SqlText nvarchar(max),
            @DestinationDatabaseName nvarchar(128),
            @DestinationSchema nvarchar(128);

    SELECT
        @DataSetId = Id,
        @DestinationDatabaseName=DestinationDatabaseName,
        @DestinationSchema=DestinationSchema
    FROM dbo.DataSet
    WHERE Id = @DataSetId

    IF @DataSetId IS NULL
    BEGIN
        SET @SqlText = 'THROW 50001,''DataSet '+CAST(@DataSetId AS nvarchar)+' not found'',1;';
        EXEC(@SqlText);
    END;

    IF @DestinationDatabaseName IS NULL OR @DestinationSchema IS NULL
    BEGIN
        SET @SqlText = 'THROW 50001,''Destination database name/schema not set for DataSet '+CAST(@DataSetId AS nvarchar)+''',1;';
        EXEC(@SqlText);
    END;

    DECLARE TableCursor CURSOR LOCAL STATIC READ_ONLY FORWARD_ONLY FOR
    SELECT
        id
        ,Name
    FROM dbo.DataTable
    WHERE DataSetId = @DataSetId

    DECLARE @TableId int;
    DECLARE @TableName varchar(50);

    OPEN TableCursor
    FETCH NEXT FROM TableCursor INTO @TableId, @TableName

    WHILE @@FETCH_STATUS = 0
    BEGIN

        PRINT '---- Table '+CAST(@TableId as varchar)+': '+@TableName
        RAISERROR('',0,1) WITH NOWAIT

        SELECT
            @SqlText = dbo.BuildCreateTableScript(
                @DestinationDatabaseName,
                @DestinationSchema,
                @TableId,
                @ForceRecreate
            )

        PRINT @SqlText
        EXEC(@SqlText)

        FETCH NEXT FROM TableCursor INTO @TableId, @TableName
    END
    CLOSE TableCursor
    DEALLOCATE TableCursor

    PRINT 'All finished.'

END
GO
