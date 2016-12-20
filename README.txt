Util program to execute hive query, get the results and its time breakup.

JDBCExecutor can be run as follows

1. mvn clean package
2. "java -jar target/*.jar --connectUrl "jdbc:hive2://localhost:10000/rajesh" --sqlFile test.sql"
 		where "test.sql" contains sql statements. Note that the sql statements have to be in single
 		line ending with ";"
3. HIVE_15388 simulates the Hive parser issue listed in HIVE-15388 jira. E.g Sql is given below.

SQL : select * from HIVE_15388 where (((((((((((((i = 0) OR i = 0) OR i = 0) OR i = 0) OR i = 0) OR i = 0) OR i = 0) OR i = 0) OR i = 0) OR i = 0) OR i = 0) OR i = 0) OR i = 0) OR i = 0
