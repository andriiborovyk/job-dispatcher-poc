# job-dispatcher

To build docker image with maven, use 
```bash
mvn clean intall -Pdocker
```

After an image built, use docker-compose file to run example: 

```bash
docker-compose up -d
```