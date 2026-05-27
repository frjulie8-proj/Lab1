# Lab 1 : Restructuring Lightcast Job Postings Data into Relational Tables
This lab1 restructures a large Lightcast job postings dataset into

6 normalized relational tables using Apache Spark (PySpark) on an AWS EC2 instance. 

The raw dataset contains over 100 columns, which are reorganized into focused tables: 
Job_Postings, Company, Job_Location, SOC_Details, LOT_Details, and NAICS_Details. 

Each table is linked by a unique job ID, enabling efficient querying and analysis. 

The restructured tables are saved as independent CSV files and compressed for submission.
