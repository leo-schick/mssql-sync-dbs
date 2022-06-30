CREATE PROC UpdateDataTableExecutionStatistic
    @NoOfBatches int = 1
AS
BEGIN

    IF @NoOfBatches < 1
        SET @NoOfBatches = 1;

    WITH total_time AS (
        SELECT
            DataTable.DataSetId
            ,DataTable.ExecutionLoadGroup
            ,SUM(LastExecutionLog.DurationInMilliseconds) AS DurationInMilliseconds
        FROM dbo.DataTable
        OUTER APPLY (
                    SELECT
                        AVG(DurationInMilliseconds) AS DurationInMilliseconds
                    FROM (
                        SELECT TOP 5 *
                        FROM ExecutionLog
                        WHERE ExecutionLog.DataTableId = DataTable.Id
                        AND ExecutionLog.TypeId = 11 -- EndLoadDataTable
                        ORDER BY ExecutionLog.DateTime DESC
                    ) AS SQ
                ) AS LastExecutionLog
        GROUP BY DataTable.DataSetId, DataTable.ExecutionLoadGroup)
    ,to_update AS (
        SELECT
            DataTable.DataSetId
            ,DataTable.ExecutionLoadGroup
            ,DataTable.ExecutionLoadOrder
            ,DataTable.ExecutionBatchId
            ,ROW_NUMBER() OVER (PARTITION BY DataTable.DataSetId, DataTable.ExecutionLoadGroup ORDER BY ISNULL(LastExecutionLog.DurationInMilliseconds,0) DESC)
                AS NewExecutionLoadOrder

            ,CEILING(
                SUM(ISNULL(LastExecutionLog.DurationInMilliseconds,0)) OVER (PARTITION BY DataTable.DataSetId, DataTable.ExecutionLoadGroup ORDER BY ISNULL(LastExecutionLog.DurationInMilliseconds,0) ASC)
                /(total_time.DurationInMilliseconds/CAST(@NoOfBatches AS numeric(28,2)))
            ) AS NewExecutionBatchId
        FROM dbo.DataTable
        LEFT JOIN total_time ON 
        total_time.DataSetId = DataTable.DataSetId
        AND total_time.ExecutionLoadGroup = DataTable.ExecutionLoadGroup
        OUTER APPLY (
            SELECT
                AVG(DurationInMilliseconds) AS DurationInMilliseconds
            FROM (
                SELECT TOP 5 *
                FROM ExecutionLog
                WHERE ExecutionLog.DataTableId = DataTable.Id
                AND ExecutionLog.TypeId = 11 -- EndLoadDataTable
                ORDER BY ExecutionLog.DateTime DESC
            ) AS SQ
        ) AS LastExecutionLog
    )
    UPDATE to_update
    SET ExecutionLoadOrder = NewExecutionLoadOrder,
        ExecutionBatchId = CASE WHEN NewExecutionBatchId > @NoOfBatches THEN @NoOfBatches ELSE NewExecutionBatchId END;

END
GO
