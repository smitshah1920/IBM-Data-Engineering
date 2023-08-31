# Data Engineering Capstone Project

#Environment:
This document introduces you to the data platform architecture of an ecommerce company named SoftCart.
SoftCart uses a hybrid architecture, with some of its databases on premises and some on cloud.

# Tools and Technologies:
OLTP database - MySQL <br>
NoSql database - MongoDB <br>
Production Data warehouse – DB2 on Cloud <br>
Staging Data warehouse – PostgreSQL <br>
Big data platform - Hadoop <br>
Big data analytics platform – Spark <br>
Business Intelligence Dashboard - IBM Cognos Analytics <br>
Data Pipelines - Apache Airflow <br>

# Process:

SoftCart's online presence is primarily through its website, which customers access using a variety of devices like laptops, mobiles andtablets. <br>

All the catalog data of the products is stored in the MongoDB NoSQL server. <br>

All the transactional data like inventory and sales are stored in the MySQL database server. <br>

SoftCart's webserver is driven entirely by these two databases. <br>

Data is periodically extracted from these two databases and put into the staging data warehouse running on PostgreSQL. <br>

The production data warehouse is on the cloud instance of IBM DB2 server. <br>

BI teams connect to the IBM DB2 for operational dashboard creation. IBM Cognos Analytics is used to create dashboards. <br>

SoftCart uses Hadoop cluster as its big data platform where all the data is collected for analytics purposes. <br>

Spark is used to analyse the data on the Hadoop cluster. <br>

To move data between OLTP, NoSQL and the data warehouse, ETL pipelines are used and these run on Apache Airflow <br>
