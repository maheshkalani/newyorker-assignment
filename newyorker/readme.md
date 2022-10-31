## Design Approach
 - Implemented configuration based framework to ingest json events to data lake.
 - There are two steps involved.
			 - First level of ingestion which is driving through `/ingestion` folder. The raw jsons will be ingested and output will be saved in `/output/{entitiy}` folder.
			 - Aggregation layer through `/aggregation` folder whose input is the output from last step. Aggregated data will be stored in `output/aggregated/{aggregated_enitity}`
 - Since the raw data is scattered across wide timeline and my assumption is that its one time ingestion, I didn’t consider partitioning of raw data for sake of this exercise . But in Production environment , i would partition my data on daily basis. But i have kept partitioning mechanism in my code.
 - Anyways `business` `user` and `checkin` data doesn’t  have appropriate partition column so there is no point of partitioning them for this exercise but i would love to explore some logical fields like `country` or `business catergories` to partition the data.
 - First level data will be validated by schema input present in `/ingestion/schemas` folder. That will give us full control over the schema changes (breaking or non breaking changes).
 - `coalesce(1)` is used while writing but based on volume of data , we can make changes to that. But for this exercise i have kept it simple.
 - Solution is dockerized as well . But i haven’t kept Hadoop and Hive in Dockerfile. So its just plain spark image.
 - Since there are no dependencies required for the project so `requirements.txt`
is missing.
## How to Run

 - download the `yelp_dataset` and unzip it to `/newyorker` folder . Folder structure will look like below
```
	|-- newyorker
	    |-- Dockerfile
	    |-- aggregation
	    |   |-- app.py
	    |   |-- transformation.py
	    |   |-- variables.py
	    |-- common
	    |-- yelp_dataset
	    |-- ingestion
	    |   |-- schemas
	    |   |   |-- business
	    |   |   |-- checkin
	    |   |   |-- review
	    |   |   |-- tip
	    |   |   |-- user
	    |   |   |-- schema_inputs.py
	    |   |-- utils.py
	    |   -- app.py
	    |-- readme.md
```
 - build the docker image - `docker build -t newyorker .`
 - Run below commands in order
 - For Ingestion
			 - ` docker run -d -v $(pwd):/app newyorker /opt/spark/bin/spark-submit --master local ingestion/app.py --entity user --input-path /app/yelp_dataset --output-path /app/output --execution-date "2022-10-31"`
			 - ` docker run -d -v $(pwd):/app newyorker /opt/spark/bin/spark-submit --master local ingestion/app.py --entity business --input-path /app/yelp_dataset --output-path /app/output --execution-date "2022-10-31"`
			 - ` docker run -d -v $(pwd):/app newyorker /opt/spark/bin/spark-submit --master local ingestion/app.py --entity tip --input-path /app/yelp_dataset --output-path /app/output --execution-date "2022-10-31"`
			 - ` docker run -d -v $(pwd):/app newyorker /opt/spark/bin/spark-submit --master local ingestion/app.py --entity checkin --input-path /app/yelp_dataset --output-path /app/output --execution-date "2022-10-31"`
			 - ` docker run -d -v $(pwd):/app newyorker /opt/spark/bin/spark-submit --master local ingestion/app.py --entity review --input-path /app/yelp_dataset --output-path /app/output --execution-date "2022-10-31"`

 - For aggregation
		 - ` docker run -d -v $(pwd):/app newyorker /opt/spark/bin/spark-submit --master local aggregation/app.py  --input-path /app/output --output-path /app/output/aggregated_data --execution-date "2022-10-31"`

## To imporve

 1. Write test cases
 2. Bring Hadoop and hive Environment in Dockerfile and create hive tables on top of processed data.
