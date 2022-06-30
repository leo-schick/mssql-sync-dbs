CREATE PROCEDURE LoadSingleTableFromSource
(
    @TableId int
)
-- =============================================
-- Autor:			Leonhard Schick
-- Created at:		21.09.2015
-- =============================================
-- Description:	
--   Load a single table from the source database into the destination table
--
-- @DataSetId				The id of the DataSet - see table [dbo].[DataSet]
-- @TableId					The id of the Table - see table [dbo].[DataTable]
-- @TransferMethod			The transfer method which shall be used (currenly not implemented)
-- @RecreateNecessary		Define if a recreation of the destination table structure
-- =============================================
AS
BEGIN

    IF @TableId IS NULL
    BEGIN
        THROW 50001,'LoadSingleTableFromSource: Param @TableId not given',1;
        RETURN
    END

    INSERT INTO ExecutionLog (DataTableId,TypeId,Text) VALUES (@TableId, 10,'Start load table '+CAST(@TableId AS nvarchar));
    DECLARE @StartTime datetime;
    SELECT @StartTime = GETDATE();

    DECLARE @SqlText nvarchar(max);

    PRINT 'Update table from source ...'

    DECLARE @DataSetId int;
    DECLARE @TransferMethod smallint;
    DECLARE @RecreateNecessary bit;
    DECLARE @HasIsModifedDateTimeField bit;
    DECLARE @HasPrimaryKey bit;

    SELECT
        @DataSetId = DataSetId,
        @TransferMethod = TransferMethod,
        @RecreateNecessary = _RecreateNecessary
    FROM dbo.DataTable
    WHERE Id = @TableId;

    IF @DataSetId IS NULL
    BEGIN
        INSERT INTO ExecutionLog (DataTableId,TypeId,Text) VALUES (@TableId, 3,'Could not find table by Id: '+CAST(@TableId AS nvarchar));
        SET @SqlText = 'THROW 50001,''Could not find table by Id: '+@TableId+''',1;';
        EXEC(@SqlText);
    END;

    DECLARE @AutoUpdateSchema bit;
    DECLARE @SourceDatabaseName nvarchar(128);
    DECLARE @SourceSchema nvarchar(128);
    DECLARE @SourceLinkedServer nvarchar(128);
    DECLARE @SourceProviderName nvarchar(128);
    DECLARE @SourceProviderString nvarchar(255);
    DECLARE @DestinationDatabaseName nvarchar(128);
    DECLARE @DestinationSchema nvarchar(128);

    SELECT
        @AutoUpdateSchema = AutoUpdateSchema,
        @SourceDatabaseName = SourceDatabaseName,
        @SourceSchema = SourceSchema,
        @SourceLinkedServer = SourceLinkedServer,
        @SourceProviderName = SourceProviderName,
        @SourceProviderString = SourceProviderString,
        @DestinationDatabaseName = DestinationDatabaseName,
        @DestinationSchema = DestinationSchema
    FROM dbo.DataSet
    WHERE id = @DataSetId;

    IF @DestinationDatabaseName IS NULL OR @DestinationSchema IS NULL
    BEGIN
        INSERT INTO ExecutionLog (DataTableId,TypeId,Text) VALUES (@TableId, 3,'DataSet '+CAST(@DataSetId AS nvarchar)+':  Destination database not set');
        SET @SqlText = 'THROW 50001,''DataSet '+CAST(@DataSetId AS nvarchar)+':  Destination database not set'',1;';
        EXEC(@SqlText);
    END;

    IF @AutoUpdateSchema = 1
        EXEC dbo.UpdateDataTableFromSource
            @DataTableId = @TableId,
            @SourceDatabaseName = @SourceDatabaseName,
            @SourceSchema = @SourceSchema,
            @SourceLinkedServer = @SourceLinkedServer,
            @SourceProviderName = @SourceProviderName,
            @SourceProviderString = @SourceProviderString,
            @DestinationDatabaseName = @DestinationDatabaseName,
            @DestinationSchema = @DestinationSchema;
        
    -- update variables updated from function EXEC dbo.UpdateDataTableFromSource
    SELECT 
        @RecreateNecessary = _RecreateNecessary,
        @HasPrimaryKey = CASE WHEN EXISTS (
            SELECT 1
            FROM dbo.DataTableField
            WHERE DataTableField.DataTableId = DataTable.Id
            AND KeyIndex IS NOT NULL
        ) THEN 1 ELSE 0 END,
        @HasIsModifedDateTimeField = CASE WHEN EXISTS (
            SELECT 1
            FROM dbo.DataTableField
            WHERE DataTableField.DataTableId = DataTable.Id
            AND DataTableField.IsModifiedDateTimeField = 1
        ) THEN 1 ELSE 0 END
    FROM dbo.DataTable
    WHERE id = @TableId;

    DECLARE @SqlScript varchar(max);

    IF @@ERROR = 0
    BEGIN

        IF @RecreateNecessary IS NOT NULL AND @RecreateNecessary = 1 BEGIN
            PRINT 'Build table (force) ...'
            SELECT @SqlScript = dbo.BuildCreateTableScript(@DestinationDatabaseName,@DestinationSchema,@TableId,1)
        END ELSE BEGIN
            PRINT 'Build table if not exist ...'
            SELECT @SqlScript = dbo.BuildCreateTableScript(@DestinationDatabaseName,@DestinationSchema,@TableId,0);
        END
        
        PRINT @SqlScript
        EXEC(@SqlScript);
        
        IF @@ERROR = 0
        BEGIN
            IF @RecreateNecessary IS NOT NULL AND @RecreateNecessary = 1
                UPDATE dbo.DataTable
                SET _RecreateNecessary = 0
                WHERE id = @TableId;

            IF @TransferMethod IS NULL AND @HasPrimaryKey = 1 AND @HasIsModifedDateTimeField = 1
                SET @TransferMethod = 3;

            IF @TransferMethod = 2 BEGIN
                PRINT 'Transfer table incremental by Id ...'

                SELECT @SqlScript = [dbo].[BuildSyncTableNewByIdScript](@DestinationSchema,@TableId,@SourceDatabaseName,@SourceSchema,@SourceLinkedServer,@DestinationDatabaseName,default,0);
            END ELSE IF @TransferMethod = 3 BEGIN
                PRINT 'Transfer table via delete old and merge new/modified ...'

                SELECT @SqlScript = [dbo].[BuildSyncTableMergeScript](@DestinationSchema,@TableId,@SourceDatabaseName,@SourceSchema,@SourceLinkedServer,@SourceProviderName,@SourceProviderString,@DestinationDatabaseName);
            END ELSE BEGIN
            --IF @TransferMethod = 1 BEGIN
                PRINT 'Transfer complete table ...'

                SELECT @SqlScript = dbo.BuildSyncTableCompleteScript(@DestinationSchema,@TableId,@SourceDatabaseName,@SourceSchema,@SourceLinkedServer,@SourceProviderName,@SourceProviderString,@DestinationDatabaseName);
            --END ELSE IF @TransferMethod = 0 BEGIN
            --	PRINT 'Transfer only new rows ...'

            --	SELECT @SqlScript = dbo.BuildSyncTableOnlyNewRowsScript(@DestinationSchema,@TableId,@SourceDatabaseName,@SourceSchema,@SourceLinkedServer,@SourceProviderName,@SourceProviderString);
            --END ELSE
            --	PRINT 'ERROR: Unknown transfer method. Skip table.'
            END;

            PRINT @SqlScript
            EXEC(@SqlScript);
        END;
    END;

    INSERT INTO ExecutionLog (DataTableId,TypeId,Text,DurationInMilliseconds) VALUES (@TableId, 11,'End load table '+CAST(@TableId AS nvarchar), DATEDIFF(MS,@StartTime,GETDATE()) );
END;
GO
