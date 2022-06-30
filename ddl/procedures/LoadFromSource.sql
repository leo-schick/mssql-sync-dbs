CREATE PROCEDURE [dbo].[LoadFromSource]
(
    @DataSetId int,
    @ExecutionLoadGroup char(1) = NULL,
    @ExecutionBatchId int = NULL
)
-- =============================================
-- Autor:			Leonhard Schick
-- Created at:		21.04.2015
-- Last chagned at: 26.11.2019
-- =============================================
-- Description:	
--   Load all data from the source database in the destination tables.
--
-- @DataSetId			The id of the data set - siehe table dbo.[DataSet] as well
-- @ExecutionLoadGroup  filter on a defined load group
-- @ExecutionBatchId    filter on a batch id
-- =============================================
AS
BEGIN

    --INSERT INTO ExecutionLog (DataSetId,TypeId,Text) VALUES (@DataSetId, 12,'Start load dataset '+CAST(@DataSetId AS nvarchar));
    --DECLARE @StartTime datetime;
    --SELECT @StartTime = GETDATE();

    DECLARE @SqlText nvarchar(max);

    IF (SELECT COUNT(*)	FROM dbo.DataSet WHERE Id = @DataSetId) = 0
    BEGIN
        --INSERT INTO ExecutionLog (DataSetId,TypeId,Text) VALUES (@DataSetId, 3,'DataSet '+CAST(@DataSetId AS nvarchar)+' not found');
        SET @SqlText = 'THROW 50001,''DataSet '+CAST(@DataSetId AS nvarchar)+' not found'',1;';
        EXEC(@SqlText);
    END;

    DECLARE TableCursor CURSOR LOCAL STATIC READ_ONLY FORWARD_ONLY FOR
    SELECT
        id
        ,Name
    FROM dbo.DataTable
    WHERE DataSetId = @DataSetId
    AND (@ExecutionLoadGroup IS NULL OR ExecutionLoadGroup = @ExecutionLoadGroup)
    AND (@ExecutionBatchId IS NULL OR ExecutionBatchId = @ExecutionBatchId)
    ORDER BY ISNULL(ExecutionLoadOrder,CAST(0x7FFFFFFF as int)) ASC, id ASC

    DECLARE @TableId int;
    DECLARE @TableName varchar(50);

    OPEN TableCursor
    FETCH NEXT FROM TableCursor INTO @TableId, @TableName

    WHILE @@FETCH_STATUS = 0
    BEGIN

        PRINT 'TIME: '+CAST(CAST(GETDATE() as time) as nvarchar)
        PRINT '---- Table '+CAST(@TableId as varchar)+': '+@TableName
        RAISERROR('',0,1) WITH NOWAIT

        EXEC dbo.[LoadSingleTableFromSource]
            @TableId;

        FETCH NEXT FROM TableCursor INTO @TableId, @TableName
    END
    CLOSE TableCursor
    DEALLOCATE TableCursor

    PRINT 'TIME: '+CAST(CAST(GETDATE() as time) as nvarchar)
    PRINT 'All finished.'

    --INSERT INTO ExecutionLog (DataSetId,TypeId,Text,DurationInMilliseconds) VALUES (@DataSetId, 13,'End load dataset '+CAST(@DataSetId AS nvarchar), DATEDIFF(MS,@StartTime,GETDATE()));

END;
GO
