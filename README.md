An end-to-end data engineering project built with Apache Airflow, Python, AWS EC2, and Amazon S3. This pipeline reads, transforms, and loads Twitter data into cloud storage, fully orchestrated through a scheduled Airflow DAG deployed on EC2.

Note: This project originally used the Twitter/X API (Tweepy) for live data extraction. Following Twitter's API paywall changes in 2023, the pipeline was adapted to ingest a Kaggle Twitter dataset — preserving all ETL and orchestration logic.


🏗️ Architecture
[Data Source]        [Transform]           [Orchestration]       [Storage]
tweets.csv      ───► twitter_etl.py   ───►  Airflow DAG     ───►  Amazon S3
(Kaggle)              (Python/Pandas)        (EC2-hosted)          tweets_cleaned.csv

📁 Project Structure
Twitter_Data_Pipeline/
├── twitter_etl.py      # Core ETL logic: ingestion, transformation, S3 upload
├── twitter_dag.py      # Airflow DAG: daily scheduling and task orchestration
├── tweets.csv          # Sample Twitter dataset (sourced from Kaggle)
└── README.md

⚙️ Tech Stack
ToolPurposePython / PandasData ingestion and transformationApache AirflowPipeline orchestration and daily schedulingAWS EC2Cloud hosting for the Airflow instanceAmazon S3Cloud storage for cleaned output dataTweepy (original)Twitter API authentication and extraction

🔄 Pipeline Flow
The run_twitter_etl() function performs the following steps:

Extract — Reads tweets.csv into a Pandas DataFrame with typed columns (author, content, country, id, language, number_of_shares, number_of_likes, date_time)
Transform

Parses date_time strings into proper datetime objects
Normalises text fields (author, content, country, language) to lowercase
Drops geolocation columns (latitude, longitude)
Removes rows with any missing values


Load — Writes the cleaned DataFrame directly to S3 as tweets_cleaned.csv

The Airflow DAG (twitter_dag) wraps this function in a PythonOperator and schedules it to run daily, with 1 automatic retry on failure.

🚀 Getting Started
Prerequisites

Python 3.8+
Apache Airflow 2.x
AWS account with EC2 and S3 access

bashpip install apache-airflow pandas boto3
Local Setup

Clone the repository

bash   git clone https://github.com/AdeoluAdegboye/Twitter_Data_Pipeline.git
   cd Twitter_Data_Pipeline

Initialise Airflow

bash   export AIRFLOW_HOME=~/airflow
   airflow db init
   airflow users create --username admin --password admin \
     --firstname Admin --lastname User --role Admin --email admin@example.com

Add the DAG

bash   cp twitter_dag.py ~/airflow/dags/
   cp twitter_etl.py ~/airflow/dags/

Configure AWS credentials for S3 write access

bash   aws configure

Start Airflow

bash   airflow webserver --port 8080 &
   airflow scheduler &

Trigger the DAG at http://localhost:8080 — look for twitter_dag


AWS Deployment (EC2)

Launch an EC2 instance (Ubuntu, t2.medium or above recommended)
Install Python, pip, and Airflow on the instance
Configure AWS credentials (aws configure) for S3 access
Copy project files and place the DAGs in Airflow's DAG folder
Start the Airflow webserver and scheduler as background services
Trigger the pipeline — output will be saved to s3://your-bucket/tweets_cleaned.csv


📊 Dataset
The tweets.csv file is sourced from Kaggle and contains the following fields:
ColumnTypeDescriptionidintUnique tweet identifierauthorstrTwitter usernamecontentstrTweet textcountrystrCountry of originlanguagestrLanguage of the tweetnumber_of_likesintLike countnumber_of_sharesintRetweet/share countdate_timedatetimeTimestamp of the tweetlatitudefloatGeolocation (dropped during transform)longitudefloatGeolocation (dropped during transform)

💡 Potential Improvements

Re-enable live ingestion using the Twitter/X API (Basic tier) or Mastodon's free open API as an alternative source
Parameterise S3 paths via Airflow Variables so the bucket name and output path aren't hardcoded in the ETL script
Add data quality checks before the load step (e.g. assert non-empty DataFrame, validate column types)
Dockerise the project with a docker-compose.yml for Airflow to make local setup fully reproducible
Add Airflow email/Slack alerts on task failure (email_on_failure: True is already scaffolded in the DAG)
Extend the transformation layer — e.g. engagement rate calculation (likes / shares), language filtering, or basic sentiment scoring
Replace flat CSV output with a data warehouse (AWS Redshift or Snowflake) to enable SQL querying at scale
