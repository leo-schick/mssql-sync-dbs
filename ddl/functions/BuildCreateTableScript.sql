CREATE FUNCTION BuildCreateTableScript
(
    @DestinationDatabaseName nvarchar(max),
    @DestinationSchema nvarchar(max),
    @TableId int,
    @ForceRecreate bit = 0
)
RETURNS nvarchar(max)
BEGIN

    DECLARE @DataPerCompany bit;
    DECLARE @TableName nvarchar(max);
    DECLARE @DBTargetTableName nvarchar(max);
    DECLARE @PrefixMode bit;

    --DECLARE @CreateHashColumns int = (SELECT CreateHashColumns FROM dbo.DataTable WHERE id = @TableId)

    SELECT
        @TableName = Name,
        @DBTargetTableName = Name, -- we should use here the SourceName... but this is the name of the source system
        @PrefixMode = PrefixMode
    FROM dbo.DataTable
    WHERE id = @TableId

    DECLARE @SqlScript nvarchar(max) = '';

    IF @ForceRecreate = 0 BEGIN
        SET @SqlScript += 'IF OBJECT_ID(''['+@DestinationDatabaseName+'].['+@DestinationSchema+'].['+@DBTargetTableName+']'', ''U'') IS NULL BEGIN ';
    END ELSE BEGIN
        SET @SqlScript += 'IF OBJECT_ID(''['+@DestinationDatabaseName+'].['+@DestinationSchema+'].['+@DBTargetTableName+']'', ''U'') IS NOT NULL ';
        SET @SqlScript += '    DROP TABLE ['+@DestinationDatabaseName+'].['+@DestinationSchema+'].['+@DBTargetTableName+'];';
    END

    SET @SqlScript += 'CREATE TABLE ['+@DestinationDatabaseName+'].['+@DestinationSchema+'].['+@DBTargetTableName+'] ('
    --SET @SqlScript += 'DataSetId int NULL, '

    -- add the business key and DiffHash
    --IF @CreateHashColumns = 1
        --SET @SqlScript += 'DwhHashBk char(32) NOT NULL, DwhHashDiff char(32) NOT NULL, '

    IF @PrefixMode = 1
        SET @SqlScript += '[__PrefixId] int NOT NULL,';
    ELSE IF @PrefixMode = 2
        SET @SqlScript += '[__PrefixId] int NULL,';

    SELECT @SqlScript += QUOTENAME(Name)+' '+[SQLType]+ISNULL(' COLLATE '+Collation,'')+CASE WHEN Nullable = 1 THEN ' NULL' ELSE ' NOT NULL' END + ','
    FROM dbo.DataTableField
    WHERE DataTableField.DataTableId = @TableId
    ORDER BY id

    SET @SqlScript = LEFT(@SqlScript,LEN(@SqlScript)-1)

    IF (SELECT COUNT(*)
        FROM dbo.DataTableField
        WHERE DataTableField.DataTableId = @TableId
        AND KeyIndex IS NOT NULL AND KeyIndex > 0
        ) > 0
    BEGIN
        SET @SqlScript += ',CONSTRAINT '+QUOTENAME(@DBTargetTableName+'$0')+' PRIMARY KEY CLUSTERED (';

        IF @PrefixMode IN (1,2)
            SET @SqlScript += '[__PrefixId] ASC,';

        SELECT @SqlScript += CASE WHEN KeyIndex = 1 THEN '' ELSE ',' END + QUOTENAME(Name) + ' ASC'
        FROM dbo.DataTableField
        WHERE DataTableField.DataTableId = @TableId
        AND KeyIndex IS NOT NULL AND KeyIndex > 0
        ORDER BY KeyIndex

        SET @SqlScript += ') '
    END
    SET @SqlScript += '); '

    IF @ForceRecreate = 0 BEGIN
        SET @SqlScript += 'END';
    END

    RETURN @SqlScript
END
GO
