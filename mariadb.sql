-- nothing interesting here, just notes/commands when testing mariadb

select * from information_schema.tables;

select distinct `IS_NULLABLE`
-- `COLUMN_NAME`, `DATA_TYPE`, `IS_NULLABLE` 
from information_schema.columns;

show engines;


DROP TABLE if EXISTS t1;
SET system_versioning_insert_history = 0; 
SET system_versioning_alter_history = 0;

SHOW variables like 'system_versioning_%';

SET @@timestamp = DEFAULT;

CREATE OR REPLACE TABLE t1(
   id INT NOT NULL AUTO_INCREMENT,
   x VARCHAR(10),
   PRIMARY KEY(id)
) 
;

ALTER TABLE t1
 ADD COLUMN __valid_from TIMESTAMP(6) GENERATED ALWAYS AS ROW START INVISIBLE,
 ADD COLUMN __valid_to TIMESTAMP(6) GENERATED ALWAYS AS ROW END INVISIBLE,
 ADD PERIOD FOR SYSTEM_TIME(__valid_from, __valid_to),
 ADD SYSTEM VERSIONING
 PARTITION BY SYSTEM_TIME INTERVAL 1 YEAR AUTO
;

select *, __valid_from, __valid_to from t1 for system_time all;

SET system_versioning_insert_history = 1;
SET @@timestamp = UNIX_TIMESTAMP('2005-10-24'); INSERT INTO t1 VALUES(1, 's');
SET @@timestamp = UNIX_TIMESTAMP('2015-10-24'); REPLACE INTO t1 VALUES(1, 't');
SET @@timestamp = UNIX_TIMESTAMP('2025-10-24'); REPLACE INTO t1 VALUES(1, 'u');
SET @@timestamp = UNIX_TIMESTAMP('2035-10-24'); REPLACE INTO t1 VALUES(1, 'v');


select *, __valid_from, __valid_to from t1 for system_time all;
