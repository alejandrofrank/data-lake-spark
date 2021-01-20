# Data Lake

In this project, we build an ETL pipeline that transforms data from an S3 bucket into another S3bucket with a better structure for analytical needs. We use Spark and AWS S3 buckets for this project.

## Required packages
Python 3 is required for this project.
```bash
pip install configparser
pip install pyspark 
```

## Usage
There is an ETL python script "etl.py" that reads data from an S3 bucket, transforms the data and pushes it to another S3 with a completely different structure easier for business analyst to consume.

1. You will need to have an S3 bucket with all the permissions enabled for this project to run. 
2. Edit the dl.cfg file in the directory and fill in the AWS id and key with yours to replicate it.
3. Go to "etl.py" file and edit the output_data variable from ```s3a://alex-udacity-dlp-test/``` to ```s3a://your-bucket-path/```
4. Save both files
5. Open a terminal and run the file "etl.py" using python3
```bash
python3 etl.py
```
6. Enjoy and analyze your song data!



## Diagram of the project
To summarize the project and give a little overiew, this diagram will help you understand it at the higher level.
![Screenshot](sparkify_data_spark_etl_architecture.png.png)

This project in a real company (I know, I'm not the real sparkify company, crazy, right?) would have a more complex architecture involving EMR cluster, maybe some lambdas that execute certain business needs that doesn't require the whole ETL or maybe some other things specially to meet the business needs. Since this project was to kind of get to know Spark's transformation and analytical power without spending a lot of money like a real company, we used little to no computing power in the cloud.
Overall, I really enjoyed this project.

Bye!