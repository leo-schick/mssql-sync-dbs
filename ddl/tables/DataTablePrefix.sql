CREATE TABLE DataTablePrefix
(
    Id int IDENTITY(1,1) NOT NULL,
    DataSetId int NOT NULL,
    Name nvarchar(50) NOT NULL,
    SourcePrefix nvarchar(50) NULL
)
GO
