# yelp_project
practice project with gcp

YELP PROJECT

MAIN MISSION: Wish an input file in JSON format, extract its data in a CSV file. 

Step 1: Cloud Storage >> Dataflow (for converting JSON to CSV)

Creat a Bucket, upload the piese of the JSON file as the test.json to the Bucket. 
Create a pipeline with Apache Beam in Python, and run it in Google Cloud Dataflow. 
This pipeline will read a JSON file which is stored in Google Cloud Storage, then extract the data and write as a CSV file output and storage it back in the Google Cloud Storage. 

Still working on Step 1 as I got the CSV but not well formatted, need to improve the extracting and formatting steps. 
(Python script: https://github.com/char-jlwu/yelp_project/blob/main/test_pipeline.py)

Step 2: Cloud Storage >> BigQuery (for storing data as structured datasets)

Create a Dataset and table(s) in BigQuery which is read directly from the CSV(s) from the previous step. 
Handling the column name, format, string characters or marks using SQL in BigQuery.
Export the well-cleaned table(s) to CSV for data analysis.  

Step 3: BigQuery >> Tableau (for data visualization)
