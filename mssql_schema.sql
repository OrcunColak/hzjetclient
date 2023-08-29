IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'myworker')
BEGIN
    CREATE TABLE myworker (
        id INT NOT NULL,
        name VARCHAR(45),
        ssn VARCHAR(45),
        PRIMARY KEY (id)
    );
END;

INSERT INTO myworker (id,name,ssn) VALUES (1, 'orcun', '208');

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'myworker_backup')
BEGIN
    CREATE TABLE myworker_backup (id INT NOT NULL, name VARCHAR(45), PRIMARY KEY (id));
END;


