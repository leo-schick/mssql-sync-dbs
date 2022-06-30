CREATE PROCEDURE UpdateDataTableFromSource
(
    @DataTableId int,
    @SourceDatabaseName nvarchar(max),
    @SourceSchema nvarchar(max) = 'dbo',
    @SourceLinkedServer nvarchar(max) = NULL,
    @SourceProviderName nvarchar(max) = NULL,
    @SourceProviderString nvarchar(max) = NULL,
    @DestinationDatabaseName nvarchar(max) = NULL,
    @DestinationSchema nvarchar(max) = NULL
)
-- =============================================
-- Autor:			Leonhard Schick
-- Created at:		21.04.2015
-- Last changes at: 26.11.2019
-- =============================================
-- Description:	
--   This procedure checks the table structure from the source database
--
--   If the source structure changed, the function will try to update
--   the structure information. If that is not possible, the function
--   will throw an error.
--
-- @DataSourceId			The id of the DataSet - see table [dbo].[DataSet]
-- @SourceDatabaseName		Name of the source database
-- @SourceSchema			Name of the source schema
-- @SourceLinkedServer		The linked server name or NULL, if the connection shall not happen via a linked server
-- @SourceProviderName		(Only when @SourceLinkedServer is NULL) The name of the database provider which shall be used for the connection
-- @SourceProviderString	(Only when @SourceLinkedServer is NULL) The connection string to the source database
-- @DestinationDatabaseName	The destination databank for the DataSet
-- @DestinationSchema		The destination schema for the DataSet
-- =============================================
AS
BEGIN
    DECLARE @DataSetId int;
    DECLARE @TableSourceName nvarchar(128);
    DECLARE @DBTargetTableName nvarchar(max);
    DECLARE @PrefixMode bit;

    SELECT
        @DataSetId = DataSetId,
        @TableSourceName = SourceName,
        @DBTargetTableName = Name,
        @PrefixMode = PrefixMode
    FROM dbo.DataTable
    WHERE id = @DataTableId

    IF @PrefixMode IN (1,2)
        SET @TableSourceName = 
            (	SELECT TOP 1 SourcePrefix
                FROM dbo.DataTablePrefix
                WHERE DataSetId = @DataSetId)
            + @TableSourceName

    DECLARE @SqlScript nvarchar(max) = '';
    DECLARE @UpdateSourceName bit = 0;
    DECLARE @UpdateSqlTypes bit = 0;
    DECLARE @UpdateKeyIndex bit = 0;
    DECLARE @OutputInt int;

    -- test for missing data in DataSet
    /*IF (SELECT COUNT(*) FROM dbo.DataTableField WHERE SourceName IS NULL) > 0
    BEGIN
        SET @SqlScript = 'THROW 50001,''Im DataSet '+CAST(@DataSetId as varchar)+' in der Tabelle '+cast(@DataTableId as varchar)+' sind nicht definierte Quellspalten vorhanden.'',1;';
        EXEC (@SqlScript);
        IF @@ERROR <> 0 THROW 50001,'Ein Fehler trat auf',1;
    END*/

    IF (SELECT COUNT(*) FROM dbo.DataTableField WHERE DataTableId = @DataTableId AND SourceName IS NULL) > 0
        SET @UpdateSourceName = 1;

    IF (SELECT COUNT(*) FROM dbo.DataTableField WHERE DataTableId = @DataTableId AND SQLType IS NULL) > 0
        SET @UpdateSqlTypes = 1;

    -- test if table exist in source
    IF @SourceLinkedServer IS NULL
        IF @SourceProviderName IS NULL
            SET @SqlScript = 'IF (SELECT COUNT(*)
                FROM dbo.DataTable
                LEFT JOIN (SELECT TABLE_NAME FROM ['+@SourceDatabaseName+'].INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG = '''+@SourceDatabaseName+''' AND TABLE_SCHEMA='''+@SourceSchema+'''
                AND TABLE_NAME = '''+@TableSourceName+''') nav ON nav.TABLE_NAME COLLATE Latin1_General_CI_AS = '''+@TableSourceName+'''
                WHERE nav.TABLE_NAME IS NULL
                AND Id='+CAST(@DataTableId as varchar)+') > 0
            THROW 50001,''DataSet '+CAST(@DataSetId as varchar)+', table '+CAST(@DataTableId as varchar)+': The table '+@SourceSchema+'.'+@TableSourceName+' does not exist in source database '+@SourceDatabaseName+''',1;'
        ELSE
            SET @SqlScript = 'IF (SELECT COUNT(*)
                    FROM dbo.DataTable
                    LEFT JOIN OPENROWSET('''+@SourceProviderName+''','''+@SourceProviderString+''',''SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG = '''''+@SourceDatabaseName+''''' AND TABLE_SCHEMA='''''+@SourceSchema+'''''
                    AND TABLE_NAME = '''''+@TableSourceName+''''''') nav ON nav.TABLE_NAME = '''+@TableSourceName+'''
                    WHERE nav.TABLE_NAME IS NULL
                    AND Id='+CAST(@DataTableId as varchar)+') > 0
                THROW 50001,''DataSet '+CAST(@DataSetId as varchar)+', table '+CAST(@DataTableId as varchar)+': The table '+@SourceSchema+'.'+@TableSourceName+' does not exist in source database '+@SourceDatabaseName+''',1;'
    ELSE
        SET @SqlScript = 'IF (SELECT COUNT(*)
                FROM dbo.DataTable
                LEFT JOIN (SELECT TABLE_NAME FROM ['+@SourceLinkedServer+'].['+@SourceDatabaseName+'].INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG = '''+@SourceDatabaseName+''' AND TABLE_SCHEMA='''+@SourceSchema+'''
                AND TABLE_NAME = '''+@TableSourceName+''') nav ON nav.TABLE_NAME COLLATE Latin1_General_CI_AS = '''+@TableSourceName+'''
                WHERE nav.TABLE_NAME IS NULL
                AND Id='+CAST(@DataTableId as varchar)+') > 0
            THROW 50001,''DataSet '+CAST(@DataSetId as varchar)+', table '+CAST(@DataTableId as varchar)+': The table '+@SourceSchema+'.'+@TableSourceName+' does not exist in source database '+@SourceDatabaseName+''',1;'
    EXEC (@SqlScript);
    IF @@ERROR <> 0 THROW 50001,'Ein Fehler trat auf',1;


    -- test for undefined rows in source
    IF @SourceLinkedServer IS NULL
        IF @SourceProviderName IS NULL
            SET @SqlScript = 'IF (SELECT COUNT(*)
                FROM dbo.DataTableField
                LEFT JOIN (SELECT COLUMN_NAME FROM ['+@SourceDatabaseName+'].INFORMATION_SCHEMA.COLUMNS WHERE TABLE_CATALOG = '''+@SourceDatabaseName+''' AND TABLE_SCHEMA='''+@SourceSchema+'''
                AND TABLE_NAME = '''+@TableSourceName+''') nav ON nav.COLUMN_NAME COLLATE Latin1_General_CI_AS = ISNULL(DataTableField.SourceName,DataTableField.Name)
                WHERE nav.COLUMN_NAME IS NULL
                AND DataTableId='+CAST(@DataTableId as varchar)+') > 0
            THROW 50001,''Im DataSet '+CAST(@DataSetId as varchar)+' in der Tabelle '+CAST(@DataTableId as varchar)+' sind undefinierte Spalten vorhanden.'',1;'
        ELSE
            SET @SqlScript = 'IF (SELECT COUNT(*)
                    FROM dbo.DataTableField
                    LEFT JOIN OPENROWSET('''+@SourceProviderName+''','''+@SourceProviderString+''',''SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_CATALOG = '''''+@SourceDatabaseName+''''' AND TABLE_SCHEMA='''''+@SourceSchema+'''''
                    AND TABLE_NAME = '''''+@TableSourceName+''''''') nav ON nav.COLUMN_NAME = ISNULL(DataTableField.SourceName,DataTableField.Name)
                    WHERE nav.COLUMN_NAME IS NULL
                    AND DataTableId='+CAST(@DataTableId as varchar)+') > 0
                THROW 50001,''Im DataSet '+CAST(@DataSetId as varchar)+' in der Tabelle '+CAST(@DataTableId as varchar)+' sind undefinierte Spalten vorhanden.'',1;'
    ELSE
        SET @SqlScript = 'IF (SELECT COUNT(*)
                FROM dbo.DataTableField
                LEFT JOIN (SELECT COLUMN_NAME FROM ['+@SourceLinkedServer+'].['+@SourceDatabaseName+'].INFORMATION_SCHEMA.COLUMNS WHERE TABLE_CATALOG = '''+@SourceDatabaseName+''' AND TABLE_SCHEMA='''+@SourceSchema+'''
                AND TABLE_NAME = '''+@TableSourceName+''') nav ON nav.COLUMN_NAME COLLATE Latin1_General_CI_AS = ISNULL(DataTableField.SourceName,DataTableField.Name)
                WHERE nav.COLUMN_NAME IS NULL
                AND DataTableId='+CAST(@DataTableId as varchar)+') > 0
            THROW 50001,''Im DataSet '+CAST(@DataSetId as varchar)+' in der Tabelle '+CAST(@DataTableId as varchar)+' sind undefinierte Spalten vorhanden.'',1;'
    EXEC (@SqlScript);
    IF @@ERROR <> 0 THROW 50001,'Ein Fehler trat auf',1;

    -- test for undefined rows in destination
    SET @SqlScript = 'SELECT @rowcount=COUNT(*)
            FROM dbo.DataTableField
            LEFT JOIN (
                SELECT c.name AS COLUMN_NAME FROM ['+@DestinationDatabaseName+'].sys.tables t
                INNER JOIN ['+@DestinationDatabaseName+'].sys.columns c ON t.object_id = c.object_id
                WHERE t.name = '''+@DBTargetTableName+'''
            ) nav ON nav.COLUMN_NAME COLLATE Latin1_General_CI_AS = ISNULL(DataTableField.Name,DataTableField.SourceName)
            WHERE nav.COLUMN_NAME IS NULL
            AND DataTableId='+CAST(@DataTableId as varchar)
    EXEC sp_executesql @SqlScript,
        N'@rowcount int output', @OutputInt output;
    IF @@ERROR <> 0 THROW 50001,'Ein Fehler trat auf',1;
    IF (@OutputInt) > 0
        SET @UpdateSqlTypes = 1;

    -- test for datatype changes
    IF @SourceLinkedServer IS NULL
        IF @SourceProviderName IS NULL
            SET @SqlScript = 'SELECT @rowcount=COUNT(*) FROM dbo.DataTableField
                JOIN (SELECT COLUMN_NAME,DATA_TYPE,CHARACTER_MAXIMUM_LENGTH,NUMERIC_PRECISION,NUMERIC_SCALE FROM ['+@SourceDatabaseName+'].INFORMATION_SCHEMA.COLUMNS WHERE TABLE_CATALOG = '''+@SourceDatabaseName+''' AND TABLE_SCHEMA='''+@SourceSchema+'''
                AND TABLE_NAME = '''+@TableSourceName+''') nav ON nav.COLUMN_NAME COLLATE Latin1_General_CI_AS = ISNULL(DataTableField.SourceName,DataTableField.Name)
                WHERE DataTableId='+CAST(@DataTableId as varchar)+' AND SQLType <> CASE DATA_TYPE
                    WHEN ''nvarchar'' THEN DATA_TYPE COLLATE Latin1_General_CI_AS +''(''+CASE WHEN CHARACTER_MAXIMUM_LENGTH = -1 THEN ''max'' ELSE CAST(CHARACTER_MAXIMUM_LENGTH as nvarchar) END+'')''
                    WHEN ''varchar'' THEN DATA_TYPE COLLATE Latin1_General_CI_AS +''(''+CASE WHEN CHARACTER_MAXIMUM_LENGTH = -1 THEN ''max'' ELSE CAST(CHARACTER_MAXIMUM_LENGTH as nvarchar) END+'')''
                    WHEN ''decimal'' THEN DATA_TYPE COLLATE Latin1_General_CI_AS +''(''+CAST(NUMERIC_PRECISION as nvarchar)+'',''+CAST(NUMERIC_SCALE as nvarchar)+'')''
                    WHEN ''numeric'' THEN DATA_TYPE COLLATE Latin1_General_CI_AS +''(''+CAST(NUMERIC_PRECISION as nvarchar)+'',''+CAST(NUMERIC_SCALE as nvarchar)+'')''
                    ELSE DATA_TYPE END';
        ELSE
            SET @SqlScript = 'SELECT @rowcount=COUNT(*) FROM dbo.DataTableField
                    JOIN OPENROWSET('''+@SourceProviderName+''','''+@SourceProviderString+''',''SELECT COLUMN_NAME,DATA_TYPE,CHARACTER_MAXIMUM_LENGTH,NUMERIC_PRECISION,NUMERIC_SCALE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_CATALOG = '''''+@SourceDatabaseName+''''' AND TABLE_SCHEMA='''''+@SourceSchema+'''''
                    AND TABLE_NAME = '''''+@TableSourceName+''''' '') nav ON nav.COLUMN_NAME = ISNULL(DataTableField.SourceName,DataTableField.Name)
                    WHERE DataTableId='+CAST(@DataTableId as varchar)+' AND SQLType <> CASE DATA_TYPE
                        WHEN ''nvarchar'' THEN DATA_TYPE+''(''+CASE WHEN CHARACTER_MAXIMUM_LENGTH = -1 THEN ''max'' ELSE CAST(CHARACTER_MAXIMUM_LENGTH as nvarchar) END+'')''
                        WHEN ''varchar'' THEN DATA_TYPE+''(''+CASE WHEN CHARACTER_MAXIMUM_LENGTH = -1 THEN ''max'' ELSE CAST(CHARACTER_MAXIMUM_LENGTH as nvarchar) END+'')''
                        WHEN ''decimal'' THEN DATA_TYPE+''(''+CAST(NUMERIC_PRECISION as nvarchar)+'',''+CAST(NUMERIC_SCALE as nvarchar)+'')''
                        WHEN ''numeric'' THEN DATA_TYPE+''(''+CAST(NUMERIC_PRECISION as nvarchar)+'',''+CAST(NUMERIC_SCALE as nvarchar)+'')''
                        ELSE DATA_TYPE END';
    ELSE
        SET @SqlScript = 'SELECT @rowcount=COUNT(*) FROM dbo.DataTableField
                JOIN (SELECT COLUMN_NAME,DATA_TYPE,CHARACTER_MAXIMUM_LENGTH,NUMERIC_PRECISION,NUMERIC_SCALE FROM ['+@SourceLinkedServer+'].['+@SourceDatabaseName+'].INFORMATION_SCHEMA.COLUMNS WHERE TABLE_CATALOG = '''+@SourceDatabaseName+''' AND TABLE_SCHEMA='''+@SourceSchema+'''
                AND TABLE_NAME = '''+@TableSourceName+''') nav ON nav.COLUMN_NAME COLLATE Latin1_General_CI_AS = ISNULL(DataTableField.SourceName,DataTableField.Name)
                WHERE DataTableId='+CAST(@DataTableId as varchar)+' AND SQLType <> CASE DATA_TYPE
                    WHEN ''nvarchar'' THEN DATA_TYPE COLLATE Latin1_General_CI_AS +''(''+CASE WHEN CHARACTER_MAXIMUM_LENGTH = -1 THEN ''max'' ELSE CAST(CHARACTER_MAXIMUM_LENGTH as nvarchar) END+'')''
                    WHEN ''varchar'' THEN DATA_TYPE COLLATE Latin1_General_CI_AS +''(''+CASE WHEN CHARACTER_MAXIMUM_LENGTH = -1 THEN ''max'' ELSE CAST(CHARACTER_MAXIMUM_LENGTH as nvarchar) END+'')''
                    WHEN ''decimal'' THEN DATA_TYPE COLLATE Latin1_General_CI_AS +''(''+CAST(NUMERIC_PRECISION as nvarchar)+'',''+CAST(NUMERIC_SCALE as nvarchar)+'')''
                    WHEN ''numeric'' THEN DATA_TYPE COLLATE Latin1_General_CI_AS +''(''+CAST(NUMERIC_PRECISION as nvarchar)+'',''+CAST(NUMERIC_SCALE as nvarchar)+'')''
                    ELSE DATA_TYPE END';
    EXEC sp_executesql @SqlScript,
        N'@rowcount int output', @OutputInt output;
    IF @@ERROR <> 0 THROW 50001,'Ein Fehler trat auf',1;
    IF (@OutputInt) > 0
        SET @UpdateSqlTypes = 1;

    -- test for index changes
    IF @SourceLinkedServer IS NULL
        IF @SourceProviderName IS NULL
            SET @SqlScript = 'SELECT @rowcount=source_rowcount FROM (SELECT COUNT(*) AS source_rowcount FROM ['+@SourceDatabaseName+'].INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                JOIN ['+@SourceLinkedServer+'].['+@SourceDatabaseName+'].INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu ON
                    kcu.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA
                    and kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
                    and kcu.TABLE_SCHEMA = tc.TABLE_SCHEMA
                    and kcu.TABLE_NAME = tc.TABLE_NAME
                WHERE tc.CONSTRAINT_TYPE = ''PRIMARY KEY''
                AND tc.TABLE_CATALOG = '''+@SourceDatabaseName+'''
                AND tc.TABLE_SCHEMA = '''+@SourceSchema+'''
                AND tc.TABLE_NAME = '''+@TableSourceName+'''
                ) AS t0';
        ELSE
            SET @SqlScript = 'SELECT @rowcount=source_rowcount FROM OPENROWSET('''+@SourceProviderName+''','''+@SourceProviderString+''',''SELECT COUNT(*) AS source_rowcount FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu ON
                    kcu.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA
                    and kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
                    and kcu.TABLE_SCHEMA = tc.TABLE_SCHEMA
                    and kcu.TABLE_NAME = tc.TABLE_NAME
                WHERE tc.CONSTRAINT_TYPE = ''''PRIMARY KEY''''
                AND tc.TABLE_CATALOG = '''''+@SourceDatabaseName+'''''
                AND tc.TABLE_SCHEMA = '''''+@SourceSchema+'''''
                AND tc.TABLE_NAME = '''''+@TableSourceName+'''''
                '') AS t0';
    ELSE
        SET @SqlScript = 'SELECT @rowcount=source_rowcount FROM (SELECT COUNT(*) AS source_rowcount FROM ['+@SourceLinkedServer+'].['+@SourceDatabaseName+'].INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            JOIN ['+@SourceLinkedServer+'].['+@SourceDatabaseName+'].INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu ON
                kcu.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA
                and kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
                and kcu.TABLE_SCHEMA = tc.TABLE_SCHEMA
                and kcu.TABLE_NAME = tc.TABLE_NAME
            WHERE tc.CONSTRAINT_TYPE = ''PRIMARY KEY''
            AND tc.TABLE_CATALOG = '''+@SourceDatabaseName+'''
            AND tc.TABLE_SCHEMA = '''+@SourceSchema+'''
            AND tc.TABLE_NAME = '''+@TableSourceName+'''
            ) AS t0';
    EXEC sp_executesql @SqlScript,
        N'@rowcount int output', @OutputInt output;
    IF @@ERROR <> 0 THROW 50001,'Ein Fehler trat auf',1;
    IF (SELECT COUNT(*)
        FROM dbo.DataTableField
        WHERE DataTableId = @DataTableId
        AND KeyIndex IS NOT NULL AND KeyIndex > 0
        ) <> @OutputInt --+ CASE WHEN @PrefixMode IN (1,2) THEN 1 ELSE 0 END
        SET @UpdateKeyIndex = 1;

    -- returns something when key is not equal
    IF @SourceLinkedServer IS NULL
        IF @SourceProviderName IS NULL
            SET @SqlScript = 'SELECT @rowcount=COUNT(*)
                FROM dbo.DataTableField
                LEFT JOIN (SELECT COLUMN_NAME, ORDINAL_POSITION
            FROM ['+@SourceDatabaseName+'].INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            JOIN ['+@SourceDatabaseName+'].INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu ON
                kcu.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA
                and kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
                and kcu.TABLE_SCHEMA = tc.TABLE_SCHEMA
                and kcu.TABLE_NAME = tc.TABLE_NAME
            WHERE tc.CONSTRAINT_TYPE = ''PRIMARY KEY''
            AND tc.TABLE_CATALOG = '''+@SourceDatabaseName+'''
            AND tc.TABLE_SCHEMA = '''+@SourceSchema+'''
            AND tc.TABLE_NAME = '''+@TableSourceName+''') AS fks ON
                    fks.COLUMN_NAME COLLATE Latin1_General_CI_AS = ISNULL(DataTableField.SourceName,DataTableField.Name) AND
                    fks.ORDINAL_POSITION = KeyIndex
                WHERE DataTableId = '+CAST(@DataTableId as varchar)+'
                AND KeyIndex IS NOT NULL AND KeyIndex > 0
                AND fks.COLUMN_NAME IS NULL';
        ELSE
            SET @SqlScript = 'SELECT @rowcount=COUNT(*)
                FROM dbo.DataTableField
                LEFT JOIN OPENROWSET('''+@SourceProviderName+''','''+@SourceProviderString+''',''SELECT COLUMN_NAME, ORDINAL_POSITION
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu ON
                kcu.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA
                and kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
                and kcu.TABLE_SCHEMA = tc.TABLE_SCHEMA
                and kcu.TABLE_NAME = tc.TABLE_NAME
            WHERE tc.CONSTRAINT_TYPE = ''''PRIMARY KEY''''
            AND tc.TABLE_CATALOG = '''''+@SourceDatabaseName+'''''
            AND tc.TABLE_SCHEMA = '''''+@SourceSchema+'''''
            AND tc.TABLE_NAME = '''''+@TableSourceName+''''''') AS fks ON
                    fks.COLUMN_NAME = ISNULL(DataTableField.SourceName,DataTableField.Name) AND
                    fks.ORDINAL_POSITION = KeyIndex
                WHERE DataTableId = '+CAST(@DataTableId as varchar)+'
                AND KeyIndex IS NOT NULL AND KeyIndex > 0
                AND fks.COLUMN_NAME IS NULL';
    ELSE
        SET @SqlScript = 'SELECT @rowcount=COUNT(*)
            FROM dbo.DataTableField
            LEFT JOIN (SELECT COLUMN_NAME, ORDINAL_POSITION
        FROM ['+@SourceLinkedServer+'].['+@SourceDatabaseName+'].INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
        JOIN ['+@SourceLinkedServer+'].['+@SourceDatabaseName+'].INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu ON
            kcu.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA
            and kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
            and kcu.TABLE_SCHEMA = tc.TABLE_SCHEMA
            and kcu.TABLE_NAME = tc.TABLE_NAME
        WHERE tc.CONSTRAINT_TYPE = ''PRIMARY KEY''
        AND tc.TABLE_CATALOG = '''+@SourceDatabaseName+'''
        AND tc.TABLE_SCHEMA = '''+@SourceSchema+'''
        AND tc.TABLE_NAME = '''+@TableSourceName+''') AS fks ON
                fks.COLUMN_NAME COLLATE Latin1_General_CI_AS = ISNULL(DataTableField.SourceName,DataTableField.Name) AND
                fks.ORDINAL_POSITION = KeyIndex
            WHERE DataTableId = '+CAST(@DataTableId as varchar)+'
            AND KeyIndex IS NOT NULL AND KeyIndex > 0
            AND fks.COLUMN_NAME IS NULL';
    EXEC sp_executesql @SqlScript,
        N'@rowcount int output', @OutputInt output;
    IF @@ERROR <> 0 THROW 50001,'Ein Fehler trat auf',1;
    IF (@OutputInt) > 0
        SET @UpdateKeyIndex = 1;

    -- Update Source field from source
    IF @UpdateSourceName = 1 BEGIN
        IF @SourceLinkedServer IS NULL
            IF @SourceProviderName IS NULL
                SET @SqlScript = 'UPDATE dbo.DataTableField
                    SET SourceName = src.COLUMN_NAME
                    FROM dbo.DataTableField
                    JOIN (SELECT COLUMN_NAME FROM ['+@SourceDatabaseName+'].INFORMATION_SCHEMA.COLUMNS WHERE TABLE_CATALOG = '''+@SourceDatabaseName+''' AND TABLE_SCHEMA='''+@SourceSchema+'''
                    AND TABLE_NAME = '''+@TableSourceName+''') src
                        ON src.COLUMN_NAME COLLATE Latin1_General_CI_AS = DataTableField.Name
                    WHERE DataTableId='+CAST(@DataTableId as varchar)+' AND DataTableField.SourceName IS NULL';
            ELSE
                SET @SqlScript = 'UPDATE dbo.DataTableField
                    SET SourceName = src.COLUMN_NAME
                    FROM dbo.DataTableField
                    JOIN OPENROWSET('''+@SourceProviderName+''','''+@SourceProviderString+''',''SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_CATALOG = '''+@SourceDatabaseName+''' AND TABLE_SCHEMA='''+@SourceSchema+'''
                    AND TABLE_NAME = '''+@TableSourceName+''' '') src
                        ON src.COLUMN_NAME = DataTableField.Name
                    WHERE DataTableId='+CAST(@DataTableId as varchar)+' AND DataTableField.SourceName IS NULL';
        ELSE
            SET @SqlScript = 'UPDATE dbo.DataTableField
                SET SourceName = src.COLUMN_NAME
                FROM dbo.DataTableField
                JOIN (SELECT COLUMN_NAME FROM ['+@SourceLinkedServer+'].['+@SourceDatabaseName+'].INFORMATION_SCHEMA.COLUMNS WHERE TABLE_CATALOG = '''+@SourceDatabaseName+''' AND TABLE_SCHEMA='''+@SourceSchema+'''
                AND TABLE_NAME = '''+@TableSourceName+''') src
                    ON src.COLUMN_NAME COLLATE Latin1_General_CI_AS = DataTableField.Name
                WHERE DataTableId='+CAST(@DataTableId as varchar)+' AND DataTableField.SourceName IS NULL';
        EXEC (@SqlScript);
        IF @@ERROR <> 0 THROW 50001,'Ein Fehler trat auf',1;
    END;

    -- Update SqlType from Source
    IF @UpdateSqlTypes = 1 BEGIN
        IF @SourceLinkedServer IS NULL
            IF @SourceProviderName IS NULL
                SET @SqlScript = 'UPDATE dbo.DataTableField
                    SET SQLType = CASE DATA_TYPE
                            WHEN ''nvarchar'' THEN DATA_TYPE+''(''+CASE WHEN CHARACTER_MAXIMUM_LENGTH = -1 THEN ''max'' ELSE CAST(CHARACTER_MAXIMUM_LENGTH as nvarchar) END+'')''
                            WHEN ''varchar'' THEN DATA_TYPE+''(''+CASE WHEN CHARACTER_MAXIMUM_LENGTH = -1 THEN ''max'' ELSE CAST(CHARACTER_MAXIMUM_LENGTH as nvarchar) END+'')''
                            WHEN ''decimal'' THEN DATA_TYPE+''(''+CAST(NUMERIC_PRECISION as nvarchar)+'',''+CAST(NUMERIC_SCALE as nvarchar)+'')''
                            WHEN ''numeric'' THEN DATA_TYPE+''(''+CAST(NUMERIC_PRECISION as nvarchar)+'',''+CAST(NUMERIC_SCALE as nvarchar)+'')''
                        ELSE DATA_TYPE END,
                        Nullable = CASE WHEN IS_NULLABLE = ''YES'' THEN 1 ELSE 0 END
                    FROM dbo.DataTableField
                    JOIN (SELECT COLUMN_NAME,DATA_TYPE,CHARACTER_MAXIMUM_LENGTH,NUMERIC_PRECISION,NUMERIC_SCALE,IS_NULLABLE FROM ['+@SourceDatabaseName+'].INFORMATION_SCHEMA.COLUMNS WHERE TABLE_CATALOG = '''+@SourceDatabaseName+''' AND TABLE_SCHEMA='''+@SourceSchema+'''
                    AND TABLE_NAME = '''+@TableSourceName+''') src
                        ON src.COLUMN_NAME COLLATE Latin1_General_CI_AS = DataTableField.SourceName
                    WHERE DataTableId='+CAST(@DataTableId as varchar);
            ELSE
                SET @SqlScript = 'UPDATE dbo.DataTableField
                    SET SQLType = CASE DATA_TYPE
                            WHEN ''nvarchar'' THEN DATA_TYPE+''(''+CASE WHEN CHARACTER_MAXIMUM_LENGTH = -1 THEN ''max'' ELSE CAST(CHARACTER_MAXIMUM_LENGTH as nvarchar) END+'')''
                            WHEN ''varchar'' THEN DATA_TYPE+''(''+CASE WHEN CHARACTER_MAXIMUM_LENGTH = -1 THEN ''max'' ELSE CAST(CHARACTER_MAXIMUM_LENGTH as nvarchar) END+'')''
                            WHEN ''decimal'' THEN DATA_TYPE+''(''+CAST(NUMERIC_PRECISION as nvarchar)+'',''+CAST(NUMERIC_SCALE as nvarchar)+'')''
                            WHEN ''numeric'' THEN DATA_TYPE+''(''+CAST(NUMERIC_PRECISION as nvarchar)+'',''+CAST(NUMERIC_SCALE as nvarchar)+'')''
                        ELSE DATA_TYPE END,
                        Nullable = CASE WHEN IS_NULLABLE = ''YES'' THEN 1 ELSE 0 END
                    FROM dbo.DataTableField
                    JOIN OPENROWSET('''+@SourceProviderName+''','''+@SourceProviderString+''',''SELECT COLUMN_NAME,DATA_TYPE,CHARACTER_MAXIMUM_LENGTH,NUMERIC_PRECISION,NUMERIC_SCALE,IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_CATALOG = '''+@SourceDatabaseName+''' AND TABLE_SCHEMA='''+@SourceSchema+'''
                    AND TABLE_NAME = '''+@TableSourceName+''' '') src
                        ON src.COLUMN_NAME = DataTableField.SourceName
                    WHERE DataTableId='+CAST(@DataTableId as varchar);
        ELSE
            SET @SqlScript = 'UPDATE dbo.DataTableField
                SET SQLType = CASE DATA_TYPE
                            WHEN ''nvarchar'' THEN DATA_TYPE+''(''+CASE WHEN CHARACTER_MAXIMUM_LENGTH = -1 THEN ''max'' ELSE CAST(CHARACTER_MAXIMUM_LENGTH as nvarchar) END+'')''
                            WHEN ''varchar'' THEN DATA_TYPE+''(''+CASE WHEN CHARACTER_MAXIMUM_LENGTH = -1 THEN ''max'' ELSE CAST(CHARACTER_MAXIMUM_LENGTH as nvarchar) END+'')''
                        WHEN ''decimal'' THEN DATA_TYPE+''(''+CAST(NUMERIC_PRECISION as nvarchar)+'',''+CAST(NUMERIC_SCALE as nvarchar)+'')''
                        WHEN ''numeric'' THEN DATA_TYPE+''(''+CAST(NUMERIC_PRECISION as nvarchar)+'',''+CAST(NUMERIC_SCALE as nvarchar)+'')''
                    ELSE DATA_TYPE END,
                    Nullable = CASE WHEN IS_NULLABLE = ''YES'' THEN 1 ELSE 0 END
                FROM dbo.DataTableField
                JOIN (SELECT COLUMN_NAME,DATA_TYPE,CHARACTER_MAXIMUM_LENGTH,NUMERIC_PRECISION,NUMERIC_SCALE,IS_NULLABLE FROM ['+@SourceLinkedServer+'].['+@SourceDatabaseName+'].INFORMATION_SCHEMA.COLUMNS WHERE TABLE_CATALOG = '''+@SourceDatabaseName+''' AND TABLE_SCHEMA='''+@SourceSchema+'''
                AND TABLE_NAME = '''+@TableSourceName+''') src
                    ON src.COLUMN_NAME COLLATE Latin1_General_CI_AS = DataTableField.SourceName
                WHERE DataTableId='+CAST(@DataTableId as varchar);
        EXEC (@SqlScript);
        IF @@ERROR <> 0 THROW 50001,'Ein Fehler trat auf',1;
    END;

    -- Update KeyIndex from Source
    IF @UpdateKeyIndex = 1 BEGIN
        IF @SourceLinkedServer IS NULL
            IF @SourceProviderName IS NULL
                SET @SqlScript = 'UPDATE dbo.DataTableField
                    SET KeyIndex = ORDINAL_POSITION
                    FROM dbo.DataTableField
                    LEFT JOIN (SELECT COLUMN_NAME, ORDINAL_POSITION
                    FROM ['+@SourceDatabaseName+'].INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                    JOIN ['+@SourceDatabaseName+'].INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu ON
                        kcu.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA
                        and kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
                        and kcu.TABLE_SCHEMA = tc.TABLE_SCHEMA
                        and kcu.TABLE_NAME = tc.TABLE_NAME
                    WHERE tc.CONSTRAINT_TYPE = ''PRIMARY KEY''
                    AND tc.TABLE_CATALOG = '''+@SourceDatabaseName+'''
                    AND tc.TABLE_SCHEMA = '''+@SourceSchema+'''
                    AND tc.TABLE_NAME = '''+@TableSourceName+''') src
                        ON src.COLUMN_NAME COLLATE Latin1_General_CI_AS = DataTableField.SourceName
                    WHERE DataTableId='+CAST(@DataTableId as varchar);
            ELSE
                SET @SqlScript = 'UPDATE dbo.DataTableField
                    SET KeyIndex = ORDINAL_POSITION
                    FROM dbo.DataTableField
                    LEFT JOIN OPENROWSET('''+@SourceProviderName+''','''+@SourceProviderString+''',''SELECT COLUMN_NAME, ORDINAL_POSITION
                    FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                    JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu ON
                        kcu.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA
                        and kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
                        and kcu.TABLE_SCHEMA = tc.TABLE_SCHEMA
                        and kcu.TABLE_NAME = tc.TABLE_NAME
                    WHERE tc.CONSTRAINT_TYPE = ''PRIMARY KEY''
                    AND tc.TABLE_CATALOG = '''+@SourceDatabaseName+'''
                    AND tc.TABLE_SCHEMA = '''+@SourceSchema+'''
                    AND tc.TABLE_NAME = '''+@TableSourceName+''' '') src
                        ON src.COLUMN_NAME = DataTableField.SourceName
                    WHERE DataTableId='+CAST(@DataTableId as varchar);
        ELSE
            SET @SqlScript = 'UPDATE dbo.DataTableField
                SET KeyIndex = ORDINAL_POSITION
                FROM dbo.DataTableField
                LEFT JOIN (SELECT COLUMN_NAME, ORDINAL_POSITION
                FROM ['+@SourceLinkedServer+'].['+@SourceDatabaseName+'].INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                JOIN ['+@SourceLinkedServer+'].['+@SourceDatabaseName+'].INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu ON
                    kcu.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA
                    and kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
                    and kcu.TABLE_SCHEMA = tc.TABLE_SCHEMA
                    and kcu.TABLE_NAME = tc.TABLE_NAME
                WHERE tc.CONSTRAINT_TYPE = ''PRIMARY KEY''
                AND tc.TABLE_CATALOG = '''+@SourceDatabaseName+'''
                AND tc.TABLE_SCHEMA = '''+@SourceSchema+'''
                AND tc.TABLE_NAME = '''+@TableSourceName+''') src
                    ON src.COLUMN_NAME COLLATE Latin1_General_CI_AS = DataTableField.SourceName
                WHERE DataTableId='+CAST(@DataTableId as varchar);
        EXEC (@SqlScript);
        IF @@ERROR <> 0 THROW 50001,'Ein Fehler trat auf',1;
    END

    -- Update _RecreateNecessary field
    IF @UpdateKeyIndex = 1 OR @UpdateSqlTypes = 1
        UPDATE dbo.DataTable
        SET _RecreateNecessary = 1
        WHERE id = @DataTableId;
END
GO
