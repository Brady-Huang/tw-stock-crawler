# welhunt_airflow

## Run the Service
You can start the service with the following command.
```
docker-compose -f docker-compose-LocalExecutor.yml up -d
```
If you want to stop the service execute thr following command.
## Stop the Service

```
docker-compose -f docker-compose-LocalExecutor.yml stop
```

## Show All Docker Container

```
docker ps   
```

## Enter Docker Container
```
docker exec -it {docker_container} /bin/bash
```

## Install Python Custome Package
Add the python package your want to install in the `requirements.txt`.

## Import Variables

Go to http://192.168.10.104:7000/admin/variable/ and select the `variables.json` file under the folder.
