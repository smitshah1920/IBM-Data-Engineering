# Description

This section contains two parts (i) Staging Area (ii) Reporting 

## Entity Relationship Diagram (Staging)

The staging area is built in PostgreSQL to hold the periodical extracted data from the web server data (transactional data - MySQL Server and catalog data - MongoDB)
This database is used as an intermediate load for the production database built in IBM Db2 <br>
The database schema is built below:<br>

![image](https://github.com/smitshah1920/IBM-Data-Engineering/assets/116938231/fc94245b-613b-476f-bf7c-54d0437690ac)

## Production data warehouse:
The production data warehouse is built in cloud using IBM Db2 <br>
The same schema as the staging data warehouse in PostgreSQL <br>
Performed the Roll-up, Cube, Grouping Sets and Materialized Query Table (MQT). <br>
