CREATE FUNCTION BuildSyncTableNewByIdScript
(
    @DestinationSchema nvarchar(max) = NULL,
    @TableId int,
    @SourceDatabaseName nvarchar(max),
    @SourceSchema nvarchar(max) = NULL,
    @SourceLinkedServer nvarchar(max),
    @DestinationDatabaseName nvarchar(max) = NULL,
    @SourceAbstractionLayerDatabaseName nvarchar(max) = NULL,
    @InvokeTruncateTable bit = 0
)
RETURNS nvarchar(max)
BEGIN
    IF @DestinationSchema IS NULL
        SET @DestinationSchema = 'dbo';
    IF @SourceSchema IS NULL
        SET @SourceSchema = 'dbo';

    DECLARE @TableName nvarchar(max);
    DECLARE @DBSourceTableName nvarchar(max);
    DECLARE @DBSourceWhereFilter nvarchar(max);
    DECLARE @DBTargetTableName nvarchar(max);
    DECLARE @DataSetId int;
    DECLARE @SyncWithRowsetFunctions bit;
    DECLARE @PrefixMode bit;
    DECLARE @SqlSyntax nvarchar(10);
    DECLARE @SqlReadHint nvarchar(max);

    DECLARE @CreateHashColumns bit = (SELECT CreateHashColumns FROM dbo.DataTable WHERE id = @TableId)

    SELECT
        @DataSetId = DataSetId,
        @TableName = Name,
        @DBTargetTableName = Name,
        @DBSourceTableName = SourceName,
        @PrefixMode = PrefixMode,
        @DBSourceWhereFilter = SourceWhereFilter
    FROM dbo.DataTable
    WHERE id = @TableId;

    DECLARE @DataSetName nvarchar(max);
    SELECT 
        @SyncWithRowsetFunctions = [SyncWithRowsetFunctions],
        @DataSetName = Name,
        @SqlSyntax = [SqlSyntax],
        @SqlReadHint = ISNULL(' WITH ('+ReadSqlTableHint+')','')
    FROM dbo.DataSet
    WHERE id = @DataSetId;
    IF @DestinationDatabaseName IS NULL
        SET @DestinationDatabaseName = 'SourceDB-'+@DataSetName
    /*IF @SourceAbstractionLayerDatabaseName IS NULL
    BEGIN
        IF @DataSetName = 'ERP2A'
            SET @SourceAbstractionLayerDatabaseName = 'SA-ERP2'
        ELSE
            SET @SourceAbstractionLayerDatabaseName = 'SA-'+@DataSetName
    END*/

    IF @SyncWithRowsetFunctions = 1
        RETURN 'THROW 50001,''Für DataSet '+CAST(@DataSetId as varchar)+' in der Tabelle '+cast(@TableId as varchar)+' konnte das ID-Synchronisations-Script nicht erstellt werden, da für das DataSet die Funktion SyncWithRowsetFunctions aktiviert ist. Dies wird aktuell nicht unterstützt.'',1;';

    IF ((SELECT COUNT(1) FROM dbo.DataTableField WHERE DataTableId = @TableId AND SourceName IS NOT NULL AND KeyIndex > 0) != 1)
        RETURN 'THROW 50001,''Im DataSet '+CAST(@DataSetId as varchar)+' in der Tabelle '+cast(@TableId as varchar)+' muss einer und darf nicht mehr als ein Schlüsselfeld verwendet werden, wenn die inkrementelle ID-Synchronisation durchgeführt werden soll'',1;';

    DECLARE @SqlScript nvarchar(max) = '';

    DECLARE @SqlKeyFieldName nvarchar(max);
    DECLARE @SqlKeyFieldType nvarchar(max);
    SELECT TOP 1
        @SqlKeyFieldName = '"'+SourceName+'"',
        @SqlKeyFieldType = ISNULL(SQLType,'int')
    FROM dbo.DataTableField
    WHERE DataTableId = @TableId AND SourceName IS NOT NULL AND KeyIndex > 0;

    SET @SqlScript += 'DECLARE @LastId '+@SqlKeyFieldType+'; ';

    IF @InvokeTruncateTable IS NOT NULL AND @InvokeTruncateTable = 1
    BEGIN
        SET @SqlScript += 'TRUNCATE TABLE ['+@DestinationDatabaseName+'].['+@DestinationSchema+'].['+@DBTargetTableName+']; ';
    END;

    DECLARE @SqlFieldList nvarchar(max) = '';
    DECLARE @NativeSQLFieldList nvarchar(max) = '';

    SELECT @SqlFieldList +=
        '"'+SourceName+'", '
    FROM dbo.DataTableField
    WHERE DataTableId = @TableId
    AND SourceName IS NOT NULL
    ORDER BY id

    SELECT @NativeSQLFieldList +=
        CASE
            WHEN @SqlSyntax = 'MySQL'
                THEN '`'+SourceName+'`, '
            WHEN @SqlSyntax = 'OracleSQL'
                THEN '"'+SourceName+'", '
            ELSE
                '['+SourceName+'], '
        END
    FROM dbo.DataTableField
    WHERE DataTableId = @TableId
    ORDER BY id

    SET @SqlFieldList = LEFT(@SqlFieldList,LEN(@SqlFieldList)-1)
    SET @NativeSQLFieldList = LEFT(@NativeSQLFieldList,LEN(@NativeSQLFieldList)-1)

    DECLARE @InsertSqlScript nvarchar(max) = '';
    SET @InsertSqlScript += 'INSERT INTO ['+@DestinationDatabaseName+'].['+@DestinationSchema+'].['+@DBTargetTableName+'] ('--'[DataSetId], '

    -- add the business key and DiffHash
    --IF @CreateHashColumns = 1
        --SET @InsertSqlScript += '[DwhHashBk], [DwhHashDiff], ';

    IF @PrefixMode IN (1,2)
        SET @InsertSqlScript += '[__PrefixId], ';

    SET @InsertSqlScript += @SqlFieldList+') SELECT '

    DECLARE @SourceFullTableName nvarchar(max);
    IF @PrefixMode = 0 BEGIN
        
        IF CHARINDEX(' ',@DBSourceTableName) > 0 OR CHARINDEX('-',@DBSourceTableName) > 0
            SET @SourceFullTableName = '"'+@DBSourceTableName+'"';
        ELSE
            SET @SourceFullTableName = @DBSourceTableName;
        IF @SourceSchema IS NOT NULL
            IF CHARINDEX(' ',@SourceSchema) > 0 OR CHARINDEX('-',@SourceSchema) > 0
                SELECT @SourceFullTableName = '"'+@SourceSchema+'".'+@SourceFullTableName;
            ELSE
                SELECT @SourceFullTableName = @SourceSchema+'.'+@SourceFullTableName;
        IF @SourceDatabaseName IS NOT NULL
            IF CHARINDEX(' ',@SourceDatabaseName) > 0 OR CHARINDEX('-',@SourceDatabaseName) > 0
                SELECT @SourceFullTableName = '"'+@SourceDatabaseName+'".'+@SourceFullTableName;
            ELSE
                SELECT @SourceFullTableName = @SourceDatabaseName+'.'+@SourceFullTableName;

        --SET @SqlScript += 'SET @LastId = (SELECT MAX('+@SqlKeyFieldName+') FROM ['+@SourceAbstractionLayerDatabaseName+'].[sa_arc].'+QUOTENAME(@DBTargetTableName)+@SqlReadHint+' WHERE [DataSetId] = '+CAST(@DataSetId AS nvarchar)+'); ';
        SET @SqlScript += 'SET @LastId = (SELECT MAX('+@SqlKeyFieldName+') FROM ['+@DestinationDatabaseName+'].['+@DestinationSchema+'].['+@DBTargetTableName+']'+@SqlReadHint+'); ';--+' WHERE [DataSetId] = '+CAST(@DataSetId AS nvarchar)+'); ';
        SET @SqlScript += @InsertSqlScript;
        --SET @SqlScript += CAST(@DataSetId AS nvarchar) +' AS DataSetId,'

        -- add the business key and DiffHash
        --IF @CreateHashColumns = 1
        --BEGIN
            --SET @SqlScript += dbo.GetBussinesKey(@TableId,10)+', '
            --SET @SqlScript += dbo.GetDiffHash(@TableId)+', '
        --END

        SET @SqlScript += @SqlFieldList

        SET @SqlScript += ' ';
        IF @SyncWithRowsetFunctions = 1
            SET @SqlScript += 'FROM OPENQUERY('+@SourceLinkedServer+',''SELECT '+@NativeSQLFieldList+' FROM '+@SourceFullTableName+@SqlReadHint+' WHERE '+ISNULL(@DBSourceWhereFilter+' AND ','')+@SqlKeyFieldName+' > ''+ISNULL(@LastId, 0));'
        ELSE
            SET @SqlScript += 'FROM ['+@SourceLinkedServer+'].'+@SourceFullTableName+@SqlReadHint+' WHERE '+ISNULL(@DBSourceWhereFilter+' AND ','')+@SqlKeyFieldName+' > ISNULL(@LastId, 0);'

    END ELSE BEGIN

        DECLARE PrefixCursor CURSOR LOCAL STATIC READ_ONLY FORWARD_ONLY FOR
        SELECT
            id
            ,SourcePrefix
        FROM dbo.DataTablePrefix
        WHERE DataSetId = @DataSetId
        ORDER BY id

        DECLARE @PrefixId int;
        DECLARE @SourceTablePrefix nvarchar(50);

        OPEN PrefixCursor
        FETCH NEXT FROM PrefixCursor INTO @PrefixId, @SourceTablePrefix

        WHILE @@FETCH_STATUS = 0
        BEGIN
        
            SET @SourceFullTableName = @SourceTablePrefix+@DBSourceTableName;
            IF CHARINDEX(' ',@SourceFullTableName) > 0 OR CHARINDEX(' ',@SourceFullTableName) > 0
                SET @SourceFullTableName = '"'+@SourceFullTableName+'"';
            IF @SourceSchema IS NOT NULL
                IF CHARINDEX(' ',@SourceSchema) > 0 OR CHARINDEX(' ',@SourceSchema) > 0
                    SELECT @SourceFullTableName = '"'+@SourceSchema+'".'+@SourceFullTableName;
                ELSE
                    SELECT @SourceFullTableName = +@SourceSchema+'.'+@SourceFullTableName;
            IF @SourceDatabaseName IS NOT NULL
                IF CHARINDEX(' ',@SourceDatabaseName) > 0 OR CHARINDEX(' ',@SourceDatabaseName) > 0
                    SELECT @SourceFullTableName = '"'+@SourceDatabaseName+'".'+@SourceFullTableName;
                ELSE
                    SELECT @SourceFullTableName = @SourceDatabaseName+'.'+@SourceFullTableName;

            --SET @SqlScript += 'SET @LastId = (SELECT MAX('+@SqlKeyFieldName+') FROM ['+@SourceAbstractionLayerDatabaseName+'].[sa_arc].'+QUOTENAME(@DBTargetTableName)+@SqlReadHint+' WHERE [DataSetId] = '+CAST(@DataSetId AS nvarchar)+' AND __PrefixId = '+CAST(@PrefixId AS nvarchar)+'); ';
            SET @SqlScript += 'SET @LastId = (SELECT MAX('+@SqlKeyFieldName+') FROM ['+@DestinationDatabaseName+'].['+@DestinationSchema+'].['+@DBTargetTableName+']'+@SqlReadHint+' WHERE __PrefixId = '+CAST(@PrefixId AS nvarchar)+'); ';
            SET @SqlScript += @InsertSqlScript;
            --SET @SqlScript += CAST(@DataSetId AS nvarchar) +' AS DataSetId, ';

            -- add the business key and DiffHash
            --IF @CreateHashColumns = 1
            --BEGIN
                --SET @SqlScript += dbo.GetBussinesKey(@TableId,@PrefixId)+','
                --SET @SqlScript += dbo.GetDiffHash(@TableId)+','
            --END

            IF @PrefixMode IN (1,2)
                SET @SqlScript += CAST(@PrefixId AS nvarchar)+' AS [__PrefixId], ';
            SET @SqlScript += @SqlFieldList

            SET @SqlScript += ' ';
            IF @SyncWithRowsetFunctions = 1
                SET @SqlScript += 'FROM OPENQUERY('+@SourceLinkedServer+',''SELECT '+@NativeSQLFieldList+' FROM '+@SourceFullTableName+@SqlReadHint+' WHERE '+ISNULL(@DBSourceWhereFilter+' AND ','')+@SqlKeyFieldName+' > ''+ISNULL(@LastId, 0));'
            ELSE
                SET @SqlScript += 'FROM ['+@SourceLinkedServer+'].'+@SourceFullTableName+@SqlReadHint+' WHERE '+ISNULL(@DBSourceWhereFilter+' AND ','')+@SqlKeyFieldName+' > ISNULL(@LastId, 0);'

            FETCH NEXT FROM PrefixCursor INTO @PrefixId, @SourceTablePrefix
        END
        CLOSE PrefixCursor
        DEALLOCATE PrefixCursor

    END;

    RETURN @SqlScript;
END
GO
