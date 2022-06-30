CREATE TABLE DataTable
(
    Id int IDENTITY(1,1) NOT NULL,
    DataSetId int NOT NULL,
    Name nvarchar(50) NOT NULL,
    SourceName nvarchar(50),
    TransferMethod tinyint,
    _RecreateNecessary bit NOT NULL DEFAULT (1),
    PrefixMode bit NOT NULL DEFAULT (0),
    ExecutionLoadOrder int,
    CreateHashColumns bit,
    ExecutionLoadGroup char(1) DEFAULT ('C'),
    SourceWhereFilter nvarchar(250),
    ExecutionBatchId int,

    CONSTRAINT PK_DataTable PRIMARY KEY (Id)
)
GO

ALTER TABLE DataTable
    WITH CHECK
    ADD CONSTRAINT FK_DataTable_DataSet FOREIGN KEY(DataSetId)
    REFERENCES DataSet (Id)
GO

ALTER TABLE DataTable CHECK CONSTRAINT FK_DataTable_DataSet
GO
