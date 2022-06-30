CREATE TABLE ExecutionLog
(
    Id int IDENTITY(1,1) NOT NULL,
    "DateTime" datetime NOT NULL DEFAULT (getdate()),
    DataSetId int NULL,
    DataTableId int NULL,
    TypeId int NOT NULL,
    "Text" nvarchar(250) NOT NULL,
    DurationInMilliseconds int NULL,
    CONSTRAINT PK_ExecutionLog PRIMARY KEY (Id)
)
GO
