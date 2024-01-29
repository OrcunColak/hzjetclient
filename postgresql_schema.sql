CREATE TABLE IF NOT EXISTS myworker (id INT NOT NULL, name VARCHAR(45), ssn VARCHAR(45), PRIMARY KEY (id));
INSERT INTO myworker (id,name,ssn) VALUES (1, 'orcun', '208');

CREATE TABLE IF NOT EXISTS myworker_backup (id INT NOT NULL, name VARCHAR(45), PRIMARY KEY (id));


CREATE TABLE IF NOT EXISTS product (id INT NOT NULL, data VARCHAR(100), PRIMARY KEY (id));
INSERT INTO product (id,data) VALUES (1, '{
  "field1": {
    "hasEvents": true
  }
}');

INSERT INTO product (id,data) VALUES (2, '{
  "field1": {
    "hasEvents": false
  }
}');