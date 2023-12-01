-- =============================================
-- Initiate author:		Marcel.Friedrich
-- Auther: Leonhard Schick
-- Create date: 03.03.2017
-- Last change: 24.09.2020
-- Description:	Erstellt aus dem Primärschlüssel 
-- sowie aus allen anderen Spalten einen MD5 Hash
-- =============================================
CREATE PROCEDURE UpdateDwhHash
    @Database nvarchar(max),
    @Table nvarchar(max),
    @DataTableId int
AS
BEGIN
    -- SET NOCOUNT ON added to prevent extra result sets from
    -- interfering with SELECT statements.
    SET NOCOUNT ON;

    SET @Database = '[' + @Database + ']'
    SET @Table = '[' + @Table + ']'

    DECLARE @FullTableName nvarchar(max) = @Database+'.dbo.'+@Table
    DECLARE @PrefixMode int = (SELECT PrefixMode FROM dbo.DataTable WHERE id = @DataTableId)

    DECLARE @BkCols NVARCHAR(MAX)
    DECLARE @DiffCols NVARCHAR(MAX)

    DECLARE @DefaultHash NVARCHAR(MAX) = 'no_columns'
    DECLARE @SQL nvarchar(max)
        
    -- ADD COLUMNS
    IF COL_LENGTH(@FullTableName, '__bk_hash') IS NULL
        EXEC ('ALTER TABLE '+@FullTableName+' ADD __bk_hash char(32)')

    IF COL_LENGTH(@FullTableName, '__diff_hash') IS NULL
        EXEC('ALTER TABLE '+@FullTableName+' ADD __diff_hash char(32)')

    -- IF PREFIX REQUIRED HASH FOR BK
        -- CONCATE COLUMNS IN 1 LINE - CAST
    IF @PrefixMode = 1
        SET @BkCols = (SELECT
            'CAST(ISNULL([__PrefixId],N'''') AS nvarchar(255))+''|''+'+Substring(T0.MyField,0,LEN(T0.MyField)-5)
        FROM (
            SELECT CAST((
                SELECT
                    CASE WHEN SQLType = 'datetime' THEN
                        'convert(nvarchar(255), ISNULL(['+SourceName+'],N''''), 113)+''|''+'
                    ELSE
                        'CAST(ISNULL(['+SourceName + '],N'''') AS nvarchar(255))+''|''+'
                    END AS 'data()'
                FROM dbo.DataTableField WHERE DataTableId = @DataTableId AND KeyIndex > 0 ORDER BY KeyIndex ASC
                FOR XML PATH('')
            ) AS nvarchar(max)) AS MyField
        ) AS T0)
    ELSE
        SET @BkCols = (SELECT
            Substring(T0.MyField,0,LEN(T0.MyField)-5)
        FROM (
            SELECT CAST((
                SELECT
                    CASE WHEN SQLType = 'datetime' THEN
                        'convert(nvarchar(255), ISNULL(['+SourceName+'],N''''), 113)+''|''+'
                    ELSE
                        'CAST(ISNULL(['+SourceName + '],N'''') AS nvarchar(255))+''|''+'
                    END AS 'data()'
                FROM dbo.DataTableField WHERE DataTableId = @DataTableId AND KeyIndex > 0 ORDER BY KeyIndex ASC
                FOR XML PATH('')
            ) AS nvarchar(max)) AS MyField
        ) AS T0)

    -- IF NO COLUMNS FOUND USE DEFAULT HASH

    SET @DiffCols = (SELECT
        Substring(T0.MyField,0,LEN(T0.MyField)-5)
    FROM (
        SELECT 
            CAST((
                SELECT 
                    -- Decimalformat muss beim konvertieren beachtet werden
                    -- Bugfix PM02.issues.estimated_hours
                    CASE WHEN SQLType LIKE 'decimal%' THEN
                        'CAST(ISNULL(FORMAT(['+SourceName + '],''G'',''EN-US''),N'''') AS nvarchar(255))+''|''+'
                    ELSE
                        'CAST(ISNULL(['+SourceName + '],N'''') AS nvarchar(255))+''|''+'
                    END AS 'data()'
                FROM dbo.DataTableField WHERE DataTableId = @DataTableId AND (KeyIndex = 0 OR KeyIndex IS NULL)
                FOR XML PATH('')
            ) AS nvarchar(max)) AS MyField
    ) AS T0)

    IF @DiffCols = '' OR @DiffCols IS NULL
        SET @SQL = 'UPDATE '+@Database+'.[dbo].'+@Table+'
        SET __bk_hash = (Select convert(char(32) ,hashbytes(''md5'', ('+@BkCols+'))),2)),
            __diff_hash = (Select convert(char(32) ,hashbytes(''md5'', (''no_columns'')),2))'
    ELSE
        SET @SQL = 'UPDATE '+@Database+'.[dbo].'+@Table+'
        SET __bk_hash = (Select convert(char(32) ,hashbytes(''md5'', ('+@BkCols+'))),2)),
            __diff_hash = (Select convert(char(32) ,hashbytes(''md5'', ('+@DiffCols+'))),2))'

    PRINT @SQL
    EXEC (@SQL)
END
GO
