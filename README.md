# Taiwan Stock Crawler

## Run the Service
You can start the airflow service with the following command.
```
docker-compose -f docker-compose-LocalExecutor.yml up -d
```
If you want to stop airflow service execute thr following command.
## Stop the Service

```
docker-compose -f docker-compose-LocalExecutor.yml stop
```

## Enter Docker Container
```
docker exec -it {docker_container} /bin/bash
```

## Install Python Custome Package For Airflow Service
Add the python package your want to install in the `requirements.txt`.

