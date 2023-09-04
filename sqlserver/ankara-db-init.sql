USE [master]
GO

IF DB_ID('ankara') IS NOT NULL
  set noexec on

CREATE DATABASE [ankara];
GO

USE [ankara]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE LOGIN [migrator] WITH PASSWORD = 'migrator123!'
GO

CREATE SCHEMA app
GO

CREATE USER [migrator] FOR LOGIN [migrator] WITH DEFAULT_SCHEMA=[app]
GO

EXEC sp_addrolemember N'db_owner', N'migrator'
GO

--CREATE LOGIN [ankaraUser] WITH PASSWORD = 'user123!'
--GO
--
--CREATE USER [ankaraUser] FOR LOGIN [ankaraUser] WITH DEFAULT_SCHEMA=[app]
--GO

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'myworker')
BEGIN
    CREATE TABLE myworker (
        id INT NOT NULL,
        name VARCHAR(45),
        ssn VARCHAR(45),
        PRIMARY KEY (id)
    );
END;
GO

INSERT INTO myworker (id,name,ssn) VALUES (1, 'orcun', '208');

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'myworker_backup')
BEGIN
    CREATE TABLE myworker_backup (id INT NOT NULL, name VARCHAR(45), PRIMARY KEY (id));
END;
