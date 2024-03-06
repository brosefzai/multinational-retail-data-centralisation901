# multinational-retail-data-centralisation901

## Description
A description of the project: what it does, the aim of the project, and what you learned  
This is a project demonstration to showcase how one can retrieve data in various ways, clean it in pandas, and put it all together to create a robust, star-schemed database. There is also a demonstration of how to query the database using SQL to obtain business intelligence.

## Installation instructions
Run the main.py file, being careful to input where asked to.


## File structure of the project
Consists of 3 .py files all wrapped up together with a fourth __main__.py file. There is also a .sql file that contains the solutions to the tasks in Milestone 4.

- data_extraction.py contains methods that retrieve data from a variety of sources: PDF file, APIs, and an S3 bucket
- database_utils.py connecting to and uploading to our local database, and listing from our RDS database
- data_cleaning.py are the documented steps to clean each of our data sources to a stage fit to upload to our local database
- milsetone4.sql contains the answers to the 9 tasks outlined in Milestone 4 of the project.