CREATE FUNCTION BuildSyncTableMergeScript
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
    DECLARE @MatchBy int; -- 1 = match by primary key, 2 = match by increment field
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

    --IF @PrefixMode <> 0
        -- hack to throw an error at this point
        -- return cast('Currently the function BuildSyncTableMergeScript does not support loading data from tables with prefix mode <> 0' AS int);

    SELECT 
        @SyncWithRowsetFunctions = [SyncWithRowsetFunctions],
        @SqlSyntax = [SqlSyntax],
        @SqlReadHint = ISNULL(' WITH ('+ReadSqlTableHint+')','')
    FROM dbo.DataSet
    WHERE id = @DataSetId;

    DECLARE @ModifiedDateTimeField_SourceName nvarchar(max);
    DECLARE @ModifiedDateTimeField_DestName nvarchar(max);
    DECLARE @ModifiedDateTimeField_IsNullable bit;
    DECLARE @CreatedDateTimeField_SourceName nvarchar(max);
    DECLARE @CreatedDateTimeField_DestName nvarchar(max);

    SELECT TOP 1
        @ModifiedDateTimeField_SourceName = SourceName,
        @ModifiedDateTimeField_DestName = Name,
        @ModifiedDateTimeField_IsNullable = Nullable
    FROM dbo.DataTableField
    WHERE DataTableId = @TableId
    AND IsModifiedDateTimeField = 1;

    IF @ModifiedDateTimeField_IsNullable = 1
    BEGIN
        SELECT TOP 1
            @CreatedDateTimeField_SourceName = SourceName,
            @CreatedDateTimeField_DestName = Name
        FROM dbo.DataTableField
        WHERE DataTableId = @TableId
        AND IsCreatedDateTimeField = 1
        AND Nullable = 0;
    END

    IF @ModifiedDateTimeField_SourceName IS NULL
    -- hack to throw an error at this point
        return cast('The function BuildSyncTableMergeScript can only be used for tables which have an modified date time field (see column IsModifiedDateTimeField in table dbo.DataTableField).' AS int);

    IF @ModifiedDateTimeField_IsNullable = 1 AND @CreatedDateTimeField_SourceName IS NULL
        return cast('The function BuildSyncTableMergeScript requires a non-nullable created date time filed when the modified date time filed is nullable (see column IsCreatedDateTimeField in table dbo.DataTableField).' AS int);

    DECLARE @DestinationModifiedDateTimeFieldExpression nvarchar(max) = CASE WHEN @ModifiedDateTimeField_IsNullable = 1 THEN 'ISNULL('+@ModifiedDateTimeField_DestName+','+@CreatedDateTimeField_SourceName+')' ELSE @ModifiedDateTimeField_DestName END
    DECLARE @SourceModifiedDateTimeFieldExpression nvarchar(max) = CASE WHEN @ModifiedDateTimeField_IsNullable = 1 THEN 'ISNULL('+@ModifiedDateTimeField_SourceName+','+@CreatedDateTimeField_SourceName+')' ELSE @ModifiedDateTimeField_SourceName END

    IF CHARINDEX(' ',@DestinationModifiedDateTimeFieldExpression) > 0 OR CHARINDEX('-',@DestinationModifiedDateTimeFieldExpression) > 0
                SET @DestinationModifiedDateTimeFieldExpression = '"'+@DestinationModifiedDateTimeFieldExpression+'"';

    IF CHARINDEX(' ',@SourceModifiedDateTimeFieldExpression) > 0 OR CHARINDEX('-',@SourceModifiedDateTimeFieldExpression) > 0
                SET @SourceModifiedDateTimeFieldExpression = '"'+@SourceModifiedDateTimeFieldExpression+'"';

    IF EXISTS (SELECT 1 FROM dbo.DataTableField WHERE DataTableId = @TableId AND KeyIndex IS NOT NULL)
        SET @MatchBy = 1;
    ELSE IF EXISTS (SELECT 1 FROM dbo.DataTableField WHERE DataTableId = @TableId AND IsIncrementalIdField = 1)
        SET @MatchBy = 2;
    ELSE
        return cast('The function BuildSyncTableMergeScript requires that the table has either a primary key or an incremental id field(see column KeyIndex and IsIncrementalIdField in table dbo.DataTableField).' AS int);

    -- // prepare for queries

    DECLARE @DestinationDatabaseCollation nvarchar(max);
    SELECT @DestinationDatabaseCollation = collation_name
    FROM sys.databases
    WHERE [name] = @DestinationDatabaseName;

    DECLARE @NativeSQLFieldList nvarchar(max) = '';
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

    DECLARE @MatchFields nvarchar(max) = '';
    IF @MatchBy = 1 BEGIN
        -- 1 = match by primary key
        SELECT @MatchFields +=
            '[target].['+Name+'] = [source].['+SourceName+']'+
            CASE WHEN LEFT(SQLType,8)='nvarchar' OR LEFT(SQLType,7) = 'varchar' THEN ' COLLATE '+@DestinationDatabaseCollation ELSE '' END+
            ' AND '
        FROM dbo.DataTableField
        WHERE DataTableId = @TableId
        AND KeyIndex IS NOT NULL
        ORDER BY KeyIndex
    END ELSE BEGIN
        -- 2 = match by increment field
        SELECT @MatchFields +=
            '[target].['+Name+'] = [source].['+SourceName+']'+
            CASE WHEN LEFT(SQLType,8)='nvarchar' OR LEFT(SQLType,7) = 'varchar' THEN ' COLLATE '+@DestinationDatabaseCollation ELSE '' END+
            ' AND '
        FROM dbo.DataTableField
        WHERE DataTableId = @TableId
        AND IsIncrementalIdField = 1
    END;
    SET @MatchFields = LEFT(@MatchFields,LEN(@MatchFields)-3)

    DECLARE @UpdateSetFields nvarchar(max) = '';
    IF @MatchBy = 1 BEGIN
        -- 1 = match by primary key
        SELECT @UpdateSetFields +=
            '['+Name+'] = [source].['+SourceName+']'+
            CASE WHEN LEFT(SQLType,8)='nvarchar' OR LEFT(SQLType,7) = 'varchar' THEN ' COLLATE '+@DestinationDatabaseCollation ELSE '' END+
            ', '
        FROM dbo.DataTableField
        WHERE DataTableId = @TableId
        AND KeyIndex IS NULL
        ORDER BY KeyIndex
    END ELSE BEGIN
        -- 2 = match by increment field
        SELECT @UpdateSetFields +=
            '['+Name+'] = [source].['+SourceName+']'+
            CASE WHEN LEFT(SQLType,8)='nvarchar' OR LEFT(SQLType,7) = 'varchar' THEN ' COLLATE '+@DestinationDatabaseCollation ELSE '' END+
            ', '
        FROM dbo.DataTableField
        WHERE DataTableId = @TableId
        AND (IsIncrementalIdField IS NULL OR IsIncrementalIdField = 0)
    END;
    SET @UpdateSetFields = LEFT(@UpdateSetFields,LEN(@UpdateSetFields)-1);

    DECLARE @InsertColumnList nvarchar(max);
    DECLARE @InsertValueList nvarchar(max);
    DECLARE @SourceFullTableName nvarchar(max);
    DECLARE @DestinationFullTableName nvarchar(max);
    DECLARE @SqlScript nvarchar(max);

    IF @PrefixMode = 0
    BEGIN

        SET @InsertColumnList = '';--'[DataSetId], ';
        SELECT @InsertColumnList +=
            '['+Name+'], '
        FROM dbo.DataTableField
        WHERE DataTableId = @TableId
        ORDER BY id
        SET @InsertColumnList = LEFT(@InsertColumnList,LEN(@InsertColumnList)-1);

        SET @InsertValueList = '';--CAST(@DataSetId AS nvarchar)+', ';
        SELECT @InsertValueList +=
            '[source].['+SourceName+']'+
            CASE WHEN LEFT(SQLType,8)='nvarchar' OR LEFT(SQLType,7) = 'varchar' THEN ' COLLATE '+@DestinationDatabaseCollation ELSE '' END+
            ', '
        FROM dbo.DataTableField
        WHERE DataTableId = @TableId
        ORDER BY id
        SET @InsertValueList = LEFT(@InsertValueList,LEN(@InsertValueList)-1);

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

        SET @DestinationFullTableName = QUOTENAME(@DestinationDatabaseName)+'.'+ISNULL(QUOTENAME(@DestinationSchema),'')+'.'+QUOTENAME(@DBTargetTableName);

        SET @SqlScript = '';


        -- delete query for old records
        SET @SqlScript += 'DELETE [target] '
        SET @SqlScript += 'FROM '+@DestinationFullTableName+' AS [target] '
        --SET @SqlScript += 'WHERE DataSetId = '+CAST(@DataSetId AS nvarchar)+' AND'
        SET @SqlScript += 'WHERE NOT EXISTS ( SELECT 1 '

        IF @SourceLinkedServer IS NULL
            IF @SourceProviderName IS NULL
                SET @SqlScript += 'FROM '+@SourceFullTableName+@SqlReadHint+';'
            ELSE
                SET @SqlScript += 'FROM OPENROWSET('''+@SourceProviderName+''','''+@SourceProviderString+''',''SELECT '+@NativeSQLFieldList+' FROM '+@SourceFullTableName+@SqlReadHint+''')'
        ELSE
            IF @SyncWithRowsetFunctions = 1
                SET @SqlScript += 'FROM OPENQUERY('+@SourceLinkedServer+',''SELECT '+@NativeSQLFieldList+' FROM '+@SourceFullTableName+@SqlReadHint+''')'
            ELSE
                SET @SqlScript += 'FROM ['+@SourceLinkedServer+'].'+@SourceFullTableName
        SET @SqlScript += ' AS [source]'+@SqlReadHint+' WHERE '+ISNULL(@DBSourceWhereFilter+' AND ','');

        SET @SqlScript += @MatchFields + '); '

        -- get last modified date

        SET @SqlScript += 'DECLARE @LastModifiedDateTime datetime; '
        SET @SqlScript += 'SELECT @LastModifiedDateTime = MAX('+@DestinationModifiedDateTimeFieldExpression+') ' -- todo when type is big datetime use date 0001-01-01
        SET @SqlScript += 'FROM '+@DestinationFullTableName + @SqlReadHint+'; '

        SET @SqlScript += 'IF @LastModifiedDateTime IS NULL '
        SET @SqlScript += 'SET @LastModifiedDateTime = ''1753-01-01'' '

        -- merge query for new and modified records

        SET @SqlScript += 'MERGE '+@DestinationFullTableName+' AS [target] USING ( SELECT * '
        IF @SourceLinkedServer IS NULL
            IF @SourceProviderName IS NULL
                SET @SqlScript += 'FROM '+@SourceFullTableName+@SqlReadHint
            ELSE
                SET @SqlScript += 'FROM OPENROWSET('''+@SourceProviderName+''','''+@SourceProviderString+''',''SELECT '+@NativeSQLFieldList+' FROM '+@SourceFullTableName+@SqlReadHint+''')'
        ELSE
            IF @SyncWithRowsetFunctions = 1
                SET @SqlScript += 'FROM OPENQUERY('+@SourceLinkedServer+',''SELECT '+@NativeSQLFieldList+' FROM '+@SourceFullTableName+@SqlReadHint+''')'
            ELSE
                SET @SqlScript += 'FROM ['+@SourceLinkedServer+'].'+@SourceFullTableName+@SqlReadHint
        SET @SqlScript += ' WHERE '+ISNULL(@DBSourceWhereFilter+' AND ','')+''+@SourceModifiedDateTimeFieldExpression+' >= @LastModifiedDateTime) AS [source] '
        SET @SqlScript += 'ON '--'target.DataSetId = '+CAST(@DataSetId AS nvarchar)+' AND '
        SET @SqlScript += @MatchFields

        SET @SqlScript += 'WHEN MATCHED THEN UPDATE SET ';

        SET @SqlScript += @UpdateSetFields;

        SET @SqlScript += ' WHEN NOT MATCHED BY target THEN INSERT (';

        SET @SqlScript += @InsertColumnList+') VALUES (';

        SET @SqlScript += @InsertValueList+');';

    END ELSE 

    BEGIN

        --SET @InsertColumnList = '[DataSetId], [__PrefixId], ';
        SET @InsertColumnList = '[__PrefixId], ';
        SELECT @InsertColumnList +=
            '['+Name+'], '
        FROM dbo.DataTableField
        WHERE DataTableId = @TableId
        ORDER BY id
        SET @InsertColumnList = LEFT(@InsertColumnList,LEN(@InsertColumnList)-1);

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

            --SET @InsertValueList = CAST(@DataSetId AS nvarchar)+', ' + CAST(@PrefixId AS nvarchar) + ', ';
            SET @InsertValueList = CAST(@PrefixId AS nvarchar) + ', ';
            SELECT @InsertValueList +=
                '[source].['+SourceName+']'+
                CASE WHEN LEFT(SQLType,8)='nvarchar' OR LEFT(SQLType,7) = 'varchar' THEN ' COLLATE '+@DestinationDatabaseCollation ELSE '' END+
                ', '
            FROM dbo.DataTableField
            WHERE DataTableId = @TableId
            ORDER BY id
            SET @InsertValueList = LEFT(@InsertValueList,LEN(@InsertValueList)-1);

            SET @DBSourceTableName = @SourceTablePrefix+@DBSourceTableName;
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

            SET @DestinationFullTableName = QUOTENAME(@DestinationDatabaseName)+'.'+ISNULL(QUOTENAME(@DestinationSchema),'')+'.'+QUOTENAME(@DBTargetTableName);

            SET @SqlScript = '';


            -- delete query for old records
            SET @SqlScript += 'DELETE [target] '
            SET @SqlScript += 'FROM '+@DestinationFullTableName+' AS [target] '
            --SET @SqlScript += 'WHERE DataSetId = '+CAST(@DataSetId AS nvarchar)+' '
            SET @SqlScript += 'WHERE __PrefixId = '+CAST(@PrefixId AS nvarchar)+' '
            SET @SqlScript += 'AND NOT EXISTS ( SELECT 1 '

            IF @SourceLinkedServer IS NULL
                IF @SourceProviderName IS NULL
                    SET @SqlScript += 'FROM '+@SourceFullTableName+@SqlReadHint+';'
                ELSE
                    SET @SqlScript += 'FROM OPENROWSET('''+@SourceProviderName+''','''+@SourceProviderString+''',''SELECT '+@NativeSQLFieldList+' FROM '+@SourceFullTableName+@SqlReadHint+''')'
            ELSE
                IF @SyncWithRowsetFunctions = 1
                    SET @SqlScript += 'FROM OPENQUERY('+@SourceLinkedServer+',''SELECT '+@NativeSQLFieldList+' FROM '+@SourceFullTableName+@SqlReadHint+''')'
                ELSE
                    SET @SqlScript += 'FROM ['+@SourceLinkedServer+'].'+@SourceFullTableName
            SET @SqlScript += ' AS [source]'+@SqlReadHint+' WHERE '+ISNULL(@DBSourceWhereFilter+' AND ','');

            SET @SqlScript += @MatchFields + '); '

            -- get last modified date

            SET @SqlScript += 'DECLARE @LastModifiedDateTime datetime; '
            SET @SqlScript += 'SELECT @LastModifiedDateTime = MAX('+@DestinationModifiedDateTimeFieldExpression+') ' -- todo when type is big datetime use date 0001-01-01
            SET @SqlScript += 'FROM '+@DestinationFullTableName + @SqlReadHint+'; '

            SET @SqlScript += 'IF @LastModifiedDateTime IS NULL '
            SET @SqlScript += 'SET @LastModifiedDateTime = ''1753-01-01'' '

            -- merge query for new and modified records

            SET @SqlScript += 'MERGE '+@DestinationFullTableName+' AS [target] USING ( SELECT * '
            IF @SourceLinkedServer IS NULL
                IF @SourceProviderName IS NULL
                    SET @SqlScript += 'FROM '+@SourceFullTableName+@SqlReadHint
                ELSE
                    SET @SqlScript += 'FROM OPENROWSET('''+@SourceProviderName+''','''+@SourceProviderString+''',''SELECT '+@NativeSQLFieldList+' FROM '+@SourceFullTableName+@SqlReadHint+''')'
            ELSE
                IF @SyncWithRowsetFunctions = 1
                    SET @SqlScript += 'FROM OPENQUERY('+@SourceLinkedServer+',''SELECT '+@NativeSQLFieldList+' FROM '+@SourceFullTableName+@SqlReadHint+''')'
                ELSE
                    SET @SqlScript += 'FROM ['+@SourceLinkedServer+'].'+@SourceFullTableName+@SqlReadHint
            SET @SqlScript += ' WHERE '+ISNULL(@DBSourceWhereFilter+' AND ','')+''+@SourceModifiedDateTimeFieldExpression+' >= @LastModifiedDateTime) AS [source] '
            --SET @SqlScript += 'ON target.DataSetId = '+CAST(@DataSetId AS nvarchar)+' AND '
            SET @SqlScript += 'ON target.__PrefixId = '+CAST(@PrefixId AS nvarchar)+' AND '
            SET @SqlScript += @MatchFields

            SET @SqlScript += 'WHEN MATCHED THEN UPDATE SET ';

            SET @SqlScript += @UpdateSetFields;

            SET @SqlScript += ' WHEN NOT MATCHED BY target THEN INSERT (';

            SET @SqlScript += @InsertColumnList+') VALUES (';

            SET @SqlScript += @InsertValueList+');';
            FETCH NEXT FROM PrefixCursor INTO @PrefixId, @SourceTablePrefix
        END
        CLOSE PrefixCursor
        DEALLOCATE PrefixCursor

    END;

    RETURN @SqlScript;

END;
GO
