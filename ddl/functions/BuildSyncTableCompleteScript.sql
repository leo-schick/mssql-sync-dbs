CREATE FUNCTION BuildSyncTableCompleteScript
(
    @DestinationSchema nvarchar(max),
    @TableId int,
    @SourceDatabaseName nvarchar(max),
    @SourceSchema nvarchar(max),
    @SourceLinkedServer nvarchar(max) = NULL,
    @SourceProviderName nvarchar(max) = NULL,
    @SourceProviderString nvarchar(max) = NULL,
    @DestinationDatabaseName nvarchar(max) = NULL
)
RETURNS nvarchar(max)
BEGIN
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

    SELECT 
        @SyncWithRowsetFunctions = [SyncWithRowsetFunctions],
        @SqlSyntax = [SqlSyntax],
        @SqlReadHint = ISNULL(' WITH ('+ReadSqlTableHint+')','')
    FROM dbo.DataSet
    WHERE id = @DataSetId;

    DECLARE @SqlScript nvarchar(max) = '';

    SET @SqlScript += 'TRUNCATE TABLE ['+@DestinationDatabaseName+'].['+@DestinationSchema+'].['+@DBTargetTableName+']; '

    DECLARE @SourceSqlFieldList nvarchar(max) = '';
    DECLARE @TargetSqlFieldList nvarchar(max) = '';
    DECLARE @NativeSQLFieldList nvarchar(max) = '';

    SELECT @SourceSqlFieldList +=
        QUOTENAME(SourceName)+', '
    FROM dbo.DataTableField
    WHERE DataTableId = @TableId
    AND SourceName IS NOT NULL
    ORDER BY id
    SELECT @TargetSqlFieldList +=
        QUOTENAME(Name)+', '
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
                QUOTENAME(SourceName)+', '
        END
    FROM dbo.DataTableField
    WHERE DataTableId = @TableId
    ORDER BY id

    SET @SourceSqlFieldList = LEFT(@SourceSqlFieldList,LEN(@SourceSqlFieldList)-1)
    SET @TargetSqlFieldList = LEFT(@TargetSqlFieldList,LEN(@TargetSqlFieldList)-1)
    SET @NativeSQLFieldList = LEFT(@NativeSQLFieldList,LEN(@NativeSQLFieldList)-1)

    DECLARE @InsertSqlScript nvarchar(max) = '';
    SET @InsertSqlScript += 'INSERT INTO ['+@DestinationDatabaseName+'].['+@DestinationSchema+'].['+@DBTargetTableName+'] ('
    --Set @InsertSqlScript += '[DataSetId], '

    -- add the business key and DiffHash
    --IF @CreateHashColumns = 1
        --SET @InsertSqlScript += '[DwhHashBk], [DwhHashDiff], ';

    IF @PrefixMode IN (1,2)
        SET @InsertSqlScript += '[__PrefixId], ';

    SET @InsertSqlScript += @TargetSqlFieldList+') SELECT '

    DECLARE @SourceFullTableName nvarchar(max);
    IF @PrefixMode = 0 BEGIN

        SET @SqlScript += @InsertSqlScript;

        --SET @SqlScript += CAST(@DataSetId AS nvarchar) +' AS DataSetId,'

        -- add the business key and DiffHash
        --IF @CreateHashColumns = 1
        --BEGIN
            --SET @SqlScript += dbo.GetBussinesKey(@TableId,10)+', '
            --SET @SqlScript += dbo.GetDiffHash(@TableId)+', '
        --END

        SET @SqlScript += @SourceSqlFieldList

        SET @SourceFullTableName = '"'+@DBSourceTableName+'"';
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

        SET @SqlScript += ' ';
        IF @SourceLinkedServer IS NULL
            IF @SourceProviderName IS NULL
                SET @SqlScript += 'FROM '+@SourceFullTableName+@SqlReadHint+ISNULL(' WHERE '+@DBSourceWhereFilter,'')+';'
            ELSE
                SET @SqlScript += 'FROM OPENROWSET('''+@SourceProviderName+''','''+@SourceProviderString+''',''SELECT '+@NativeSQLFieldList+' FROM '+@SourceFullTableName+@SqlReadHint+ISNULL(' WHERE '+@DBSourceWhereFilter,'')+''');'
        ELSE
            IF @SyncWithRowsetFunctions = 1
                SET @SqlScript += 'FROM OPENQUERY('+@SourceLinkedServer+',''SELECT '+@NativeSQLFieldList+' FROM '+@SourceFullTableName+@SqlReadHint+ISNULL(' WHERE '+@DBSourceWhereFilter,'')+''');'
            ELSE
                SET @SqlScript += 'FROM ['+@SourceLinkedServer+'].'+@SourceFullTableName+@SqlReadHint+ISNULL(' WHERE '+@DBSourceWhereFilter,'')+';'

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
        
            SET @SqlScript += @InsertSqlScript;

            --SET @SqlScript += CAST(@DataSetId AS nvarchar) +' AS DataSetId,'

            -- add the business key and DiffHash
            --IF @CreateHashColumns = 1
            --BEGIN
                --SET @SqlScript += dbo.GetBussinesKey(@TableId,@PrefixId)+','
                --SET @SqlScript += dbo.GetDiffHash(@TableId)+','
            --END

            IF @PrefixMode IN (1,2)
                SET @SqlScript += CAST(@PrefixId AS nvarchar)+' AS [__PrefixId], ';
            SET @SqlScript += @SourceSqlFieldList

            SET @SourceFullTableName = @SourceTablePrefix+@DBSourceTableName;
            IF CHARINDEX(' ',@SourceFullTableName) > 0 OR CHARINDEX('-',@SourceFullTableName) > 0
                SET @SourceFullTableName = '"'+@SourceFullTableName+'"';
            IF @SourceSchema IS NOT NULL
                IF CHARINDEX(' ',@SourceSchema) > 0 OR CHARINDEX('-',@SourceSchema) > 0
                    SELECT @SourceFullTableName = '"'+@SourceSchema+'".'+@SourceFullTableName;
                ELSE
                    SELECT @SourceFullTableName = +@SourceSchema+'.'+@SourceFullTableName;
            IF @SourceDatabaseName IS NOT NULL
                IF CHARINDEX(' ',@SourceDatabaseName) > 0 OR CHARINDEX('-',@SourceDatabaseName) > 0
                    SELECT @SourceFullTableName = '"'+@SourceDatabaseName+'".'+@SourceFullTableName;
                ELSE
                    SELECT @SourceFullTableName = @SourceDatabaseName+'.'+@SourceFullTableName;

            SET @SqlScript += ' ';
            IF @SourceLinkedServer IS NULL
                IF @SourceProviderName IS NULL
                    SET @SqlScript += 'FROM '+@SourceFullTableName+@SqlReadHint+ISNULL(' WHERE '+@DBSourceWhereFilter,'')+';'
                ELSE
                    SET @SqlScript += 'FROM OPENROWSET('''+@SourceProviderName+''','''+@SourceProviderString+''',''SELECT '+@NativeSQLFieldList+' FROM '+@SourceFullTableName+@SqlReadHint+ISNULL(' WHERE '+@DBSourceWhereFilter,'')+''');'
            ELSE
                IF @SyncWithRowsetFunctions = 1
                    SET @SqlScript += 'FROM OPENQUERY('+@SourceLinkedServer+',''SELECT '+@NativeSQLFieldList+' FROM '+@SourceFullTableName+@SqlReadHint+ISNULL(' WHERE '+@DBSourceWhereFilter,'')+''');'
                ELSE
                    SET @SqlScript += 'FROM ['+@SourceLinkedServer+'].'+@SourceFullTableName+@SqlReadHint+ISNULL(' WHERE '+@DBSourceWhereFilter,'')+';'

            FETCH NEXT FROM PrefixCursor INTO @PrefixId, @SourceTablePrefix
        END
        CLOSE PrefixCursor
        DEALLOCATE PrefixCursor

    END;

    RETURN @SqlScript;
END
GO
