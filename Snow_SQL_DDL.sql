/*This demo imports a flat file JSON into snowflake 
and then uses ETL to move from a staging area to an ODS */

Use DATABASE JSON_Example;

Use SCHEMA Staging;


--1. since we use JSON file to upload, we need to create a JSON format
create or replace file format myjsonformat type = 'JSON' strip_outer_array=true;

--2. Create a temporary holding area for the data called a “staging file”
create or replace stage my_json_stage file_format = myjsonformat ;

--3.create a table with one column of type variant:
create table userdetails(usersjson variant) ;

--4.To upload the data from your local computer to the temporary "holding area" stagefile area
put file://desktop/userdetails.json  @my_json_stage auto_compress=true;

--5.Now finally copy the data you just uploaded directly into the table created in the previous steps
copy into userdetails from @my_json_stage/userdetails.json.gz file_format=(format_name = myjsonformat) on_error = 'skip_file';
--Press If you see the status LOADED it means the data was successfully loaded from the staging area to the table.


/* now create a table for ODS and use ETL to pull JSON into ODS in a new table format*/
--transform table into columns for json file:
create table userdetails (
    email string,
    firstname string,
    lastname string,
    phone number,
    userid number
);

insert into userdetails 
select 
usersjson:emailAddress,
usersjson:firstName,
usersjson:lastName,
usersjson:phoneNumber,
usersjson:userid
from udacityproject.staging.userdetails;

--check the results of the new ETL table in ODS
select * from userdetails;
