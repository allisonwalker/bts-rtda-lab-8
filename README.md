- Launch/Describe/Terminate AWS Instance ( Big Data Arch )

```bash
 aws ec2 run-instances --image-id ami-0b2d078af212c7b40 --count 1 --instance-type m5.xlarge --key-name <your_key_name> --security-group-ids sg-0e8cb59d207ca3ed3 --subnet-id subnet-0ba219ffbd8c264d2 --associate-public-ip-address
```

- Get public dns of EC2 instance

```bash
aws ec2 describe-instances --filter "Name=instance-id,Values=<id-instance>"
```

- Connect to EC2

```bash
ssh -i <your-key-path> ec2-user@<intance-public-dns>
``` 

- Inside instance, edit Spark Dockerfile and add a volume on spark instance definition to share data with spark container. 
```bash
vi dpcker-compose.yml
 spark:
        volumes:
            - ./data:/appdata
```

- Restart/start docker cluster

```
docker-compose stop
docker-compose rm
docker-compose build
./start-docker-compose.sh
```

- Check all nodes of the docker cluster are up

```bash
docker-compose ps
```

- If docker-compose do start due to error duplication we need to delete duplicate networks

```bash
sudo docker network ls
sudo docker network rm <network ID>

./start-docker-compose.sh
```


- Start producing mesages to kafka from meetup url, being in EC2 console do:

```bash
docker exec -it ec2-user_sensor_1 /bin/sh
curl -i http://stream.meetup.com/2/rsvps | kafkacat -b kafka:9092 -t stream
```

- Open a new terminal and enter to the amazon EC2 follow previous approach

- Enter spark container and set sbt environment variable

```bash
docker exec -it spark bash
PATH="/usr/local/sbt/bin:${PATH}"
```

- Open a new terminal and enter to the amazon EC2 follow previous approach

- Copy local app code to EC2 from a new console

```bash
scp -i <your aws key> -r $(pwd) ec2-user@ec2-35-180-189-82.eu-west-3.compute.amazonaws.com:~/data

Example:

scp -i ~/keys/liesner_key_par.pem -r $(pwd) ec2-user@ec2-35-180-189-82.eu-west-3.compute.amazonaws.com:~/data
``` 

- Go back to your spark terminal build the sbt

```bash
cd /appdata
sbt package
```  

- Submit app to spark cluster
```bash
spark-submit --master local[2] --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0,org.apache.kafka:kafka-clients:2.2.0,org.apache.spark:spark-tags_2.11:2.4.0,org.apache.spark:spark-sql_2.11:2.4.0,org.elasticsearch:elasticsearch-spark-20_2.11:7.1.1 --class Main target/scala-2.11/bts-rtda-lab-8_2.11-0.1.1.jar kafka elasticsearch meetup-topics 
```

- Check new created index "topics" on kibana and create a real time graphic.

- Exercises: Base in the boilerplate project add to the app a new pipelines:
    -  create and index in elasticserach named "membernames" wich contain the creators members names for each meetup. Create on kibana a realtime graphic that show the top members by name.
    -  create an index in elasticserach named "venuesnamelocation" which contain the venues an location (in the string format "lon,lat") for each meetup. Create on kibana a realtime graphic that show the top venues by name. 

- Assignment: Base in the boilerplate project add to the app a new pipeline
    - Add to the app a new flow to create and index on elasticserach named "eventtopicount" witch contain the event name and the count of topics that the event contain. Create on kibana a realtime graphic that show the top events based on count of topics. 

- After finish do not forget to end the EC2

```bash
aws ec2 terminate-instances --instance-ids <id-instance>
```


As references you can find manually steps here [manually steps](docs/manually.md)
