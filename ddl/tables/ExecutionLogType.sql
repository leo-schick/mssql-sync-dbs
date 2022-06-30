CREATE TABLE ExecutionLogType
(
    TypeId int NOT NULL,
    Name nvarchar(250) NOT NULL,
    CONSTRAINT PK_ExecutionLogTypeId PRIMARY KEY (TypeId)
)
GO

INSERT INTO ExecutionLogType
(TypeId, Name)
VALUES
(1, 'Info'),
(2, 'Warning'),
(3, 'Error'),
(10, 'StartLoadTable'),
(11, 'EndLoadTable'),
(12, 'StartLoadDataSet'),
(13, 'EndLoadDataSet')
GO
