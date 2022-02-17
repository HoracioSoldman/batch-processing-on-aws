## Running DAGS on Airflow
In this project, we are using Airflow in a docker container.

### Requirements
In order to run Airflow and the pipeline in this project, you need to have:

* [Docker](https://www.docker.com) and [docker compose](https://docs.docker.com/compose/install) installed. This can be checked by running 
```bash
docker -v
``` 
* [AWS Account](https://aws.amazon.com/account/) which has access to an [S3 bucket](https://aws.amazon.com/s3/)
* An **AWS_ACCESS_KEY_ID** and an **AWS_SECRET_ACCESS_KEY** associated with the [AWS Account](https://aws.amazon.com/account/). 

### Set ENVIRONMENT VARIABLES
Before building the airflow docker image, it is necessary to set ENVIRONMENT VARIABLES in a `.env` file.

To do so, rename the `.env.example` file located in this directory to `.env` then add the correct values for your own environment.

The `AIRFLOW_UID` value can be obtained from the following command:
```bash
echo -e "AIRFLOW_UID=$(id -u)" > .env
```
Then `AIRFLOW_GID` can be set to `0`.

### Dockerfile and docker-compose.yaml
In DOckerfile, we download several packages such as: 
1. [firefox esr](https://www.mozilla.org/en-US/firefox/enterprise/) for web scraping

2. [selenium](https://pypi.org/project/selenium/) for web scraping

3. [webdriver_manager](https://pypi.org/project/webdriver-manager/)  for web scraping

4. [apache-airflow-providers-amazon](https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/index.html) to communicate and work with AWS

5. [pyarrow](https://pypi.org/project/pyarrow/) to convert `.csv` to `.parquet` files

6. [bs4](https://pypi.org/project/beautifulsoup4/) for web scraping

Also, the `docker-compose.yaml` is a modified version of the original [docker-compose.yaml](https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml).


### Run Airflow

1. Build docker image with the current Dockerfile
```bash
docker build -t airflow-img .
```
This command should be run only once or after editing the content of Dockerfile.

2. Initialize Airflow
```bash
docker-compose up airflow-init
```
This command should terminate with `exit code 0` if everything went well.

3. Launch Airflow
```bash
docker-compose up
```

4. Visit [http://localhost:8080](http://localhost:8080) to access the Airflow GUI.

5. In order to stop the Airflow container,
```bash
docker-compose down
```

### Note: 
It is highly recommended to manually trigger the `web_scraping_dag` prior to enabling the `s3_ingestion_dag`. 

In order to do this, open the `web_scraping_dag` and click on the **Play** button on the right, then select **Trigger Dags now**.




