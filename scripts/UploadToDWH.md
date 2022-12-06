# How to upload a CSV file as a table to Snowflake

On the terminal, select an existing database and schema you want to create a table in:
```console
 USE DATABASE KIV;
 USE SCHEMA ACCOUNT_USAGE;
 ```

Set the input format of the timestamp using:
```console 
ALTER SESSION SET TIME_INPUT_FORMAT="YYYY-MM-DD HH24:MI:SS.FF TZH:TZM";
```

Create the table:
```console
CREATE OR REPLACE TABLE DATABASES(
                                created_on TIMESTAMP,           
                                names VARCHAR,                  
                                is_default VARCHAR,          
                                is_current VARCHAR,          
                                database_name VARCHAR,
                                owners VARCHAR,
                                Comments VARCHAR,
                                options VARCHAR,
                                retention_time NUMERIC)
```
Create an internal stage to load data into Snowflake tables:
```console
CREATE OR REPLACE STAGE optiml_stage;
```
Upload the CSV file from a local directory to to the stage:
```console 
put file://<path>/databases.csv @optiml_stage;
```
You can take a look at the uploaded files of your CSV by using this command:
```console
select d.$1,d.$2,d.$3 from @optiml_stage d;
```
We can now copy the data from our CSV file into the table we created:
```console
COPY INTO DATABASES from @optiml_stage/databases.csv
file_format=(type='csv' field_delimiter=',' skip_header=1 FIELD_OPTIONALLY_ENCLOSED_BY= '0x22');
```
> **_NOTE:_**          
- Ensure that the order of headings in the CSV file is the same as that of the table created.                              

