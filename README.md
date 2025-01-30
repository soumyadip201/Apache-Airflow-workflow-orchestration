# Apache Airflow Workflow Orchestration

## Step 1: Configure Docker Compose

In your docker-compose.yml file, under the volumes section, add the following:

```
volumes:
  - ${AIRFLOW_PROJ_DIR:-.}/dags:/opt/airflow/dags
  - ${AIRFLOW_PROJ_DIR:-.}/logs:/opt/airflow/logs
  - ${AIRFLOW_PROJ_DIR:-.}/config:/opt/airflow/config
  - ${AIRFLOW_PROJ_DIR:-.}/plugins:/opt/airflow/plugins
  - ./output:/output/
```

Make sure to replace `D:/Airflow/airflow-docker` with the environment variable `${AIRFLOW_PROJ_DIR:-.}`, which can be defined as your desired project directory path.

## Step 2: Install Docker Desktop

Download and install Docker Desktop from the official website if you haven't done so already. Follow the installation instructions provided there.

## Step 3: Start Airflow with Docker

Open your terminal and navigate to your project folder. Run the following command to start Airflow using Docker:

```
docker-compose up
```

This will spin up the Airflow services in your Docker container.

## Step 4: Access Airflow Webserver

Once the containers are running, you can access the Airflow webserver by visiting:
`localhost:8080`

## Step 5: Login Credentials

The default username and password are both set to airflow. If you wish to change these credentials, follow these steps:

- Stop the running container by typing the following in your terminal:

- `docker-compose down`

- In the docker-compose.yml file, under the environment section, find the following:

```
_AIRFLOW_WWW_USER_USERNAME: ${_AIRFLOW_WWW_USER_USERNAME:-airflow}
_AIRFLOW_WWW_USER_PASSWORD: ${_AIRFLOW_WWW_USER_PASSWORD:-airflow}
```

Change airflow to your desired username and password.

Restart the container by typing:

`docker-compose up`

## Step 6: Create Required Folders

Under the root folder of the project, create the following folders:

- dags
- logs
- config
- plugins
- output

These folders will be used by Airflow for storing various resources.

## Step 7: Add Your DAG Files

Place your custom DAG files inside the dags folder. Once added, you can check their execution status via the Airflow web interface."
