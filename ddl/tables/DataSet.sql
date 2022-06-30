CREATE TABLE DataSet
(
    Id int IDENTITY(1,1) NOT NULL,
    Name nvarchar(20) NOT NULL,
    Description nvarchar(50) NOT NULL,
    AutoUpdateSchema bit NOT NULL,
    SyncWithRowsetFunctions bit NOT NULL,
    SqlSyntax nvarchar(10),
    SourceDatabaseName nvarchar(128),
    SourceSchema nvarchar(128),
    SourceLinkedServer nvarchar(128),
    SourceProviderName nvarchar(128),
    SourceProviderString nvarchar(255),
    DestinationDatabaseName nvarchar(128),
    DestinationSchema nvarchar(128),
    ReadSqlTableHint varchar(20),

    CONSTRAINT PK_DataSet PRIMARY KEY (Id)
)
GO
