# Udacity-Data-Engineering-Projects
This repository contains the projects completed as part of the Udacity Nanodegree in Data Engineering 
The projects portray the journey in which I assumed the role of a Data Engineer at a fabricated data streaming company called “Sparkify” as it scales its data engineering in both size and sophistication. 

### Project 1 - Data Modeling
In this project, I modeled activity data for a music streaming app called Sparkify. I created a database and imported data stored in CSV and JSON files, and modeled the data with a relational model in Postgres.The data models are designed to optimize queries for understanding what songs users are listening to. 

### Project 2 - Cloud Data Warehousing
In this project, I moved to the cloud as I worked with larger amounts of data. I built an ELT pipeline that extracts Sparkify’s data from S3, staged the data in Amazon Redshift and transformed it into a set of fact and dimensional tables for the Sparkify analytics team to continue finding insights in what songs their users are listening to.

### Project 3 - Data Lakes with Apache Spark
In this project, I built an ETL pipeline for a data lake. The data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in the app. I loaded the data from S3, processed the data into analytics tables using Spark, and loaded them back into S3. This process is done by deploying a Spark cluster on AWS.

### Project 4 - Data Pipelines with Apache Airflow
In this project,I improved Sparkify’s data infrastructure by creating and automating a set of data pipelines. I configureD and schedule data pipelines with Airflow, setting dependencies, triggers, and quality checks as I would in a production setting.
