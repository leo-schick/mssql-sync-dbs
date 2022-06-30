CREATE TABLE DataTableField
(
    Id int IDENTITY(1,1) NOT NULL,
    DataTableId int NOT NULL,
    "Name" nvarchar(50) NOT NULL,
    SourceName nvarchar(50) NULL,
    SQLType varchar(50) NULL,
    KeyIndex smallint NULL,
    Nullable bit NOT NULL DEFAULT (0),
    IsModifiedDateTimeField bit NULL,
    IsCreatedDateTimeField bit NULL,
    IsIncrementalIdField bit NULL,
    Collation nvarchar(40) NULL,

    CONSTRAINT PK_DataTableField PRIMARY KEY (Id)
)
GO

ALTER TABLE DataTableField
    WITH CHECK
    ADD CONSTRAINT FK_DataTableField_DataSetTable FOREIGN KEY(DataTableId)
    REFERENCES DataTable (Id)
GO

ALTER TABLE DataTableField CHECK CONSTRAINT FK_DataTableField_DataSetTable
GO
