# Parse tweets using Kafka stream with Yarn/Samza

This is a distributed streaming application that reads tweets from twitter using Python, and pipes it to Kafka streams. Jobs are run using Yarn, Samza and Zookeeper on Hadoop 2.0




### RUNNING THE CODE

### 1.Prerequisites
 a) make sure JAVA_HOME is set in .bashrc, use printev to check.
```
printenv
```

b) Make sure you have write persmission in /home directory or wherever you are cloning the repo.
```
git clone https://github.com/bassrehab/python-kafka-streaming-twitter.git
```

c) Create a twitter app and fetch consumer token, secrets etc.


### 2.Build Samza distribution artifacts in deploy directory
```
mvn clean install
```
This may take some time. Check that you have a new /deploy directory. in deploy/bin you will find useful scripts that we will use to run Yarn jobs in a bit. deploy/conf has configuration properties for kafka streams.

### 3.Give proper permission to the source directory
```
chmod -R -f 4777 python-kafka-streaming-twitter
```

### 4.Start the yarn cluster
Install yarn, zookeeper, kafka
```
sudo sh grid intall all
```

to start zookeeper, yarn,kafka
```
sudo ./grid start all
```

### 5.Start kafka stream

pull data from twitter using python script and pipe it to kafka producer and bind to 'tweets' topic

```
python raw-stream.py | /home/deploy//kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic tweets 
```
if  you encounter error due to impropoer yarn shut down, simply delete /tmp/kafka-logs directory

### 6.From /home directory run the Samza deployment script with correct paths and attributes

in a new terminal type, to view the job submission / live twitter json objects:
```
/home/deploy/kafka/bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic tweets
```

### 7.Run the Samza deployment script with correct paths and attributes

```
sudo python-kafka-streaming-twitter/deploy/bin/run-job.sh --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory --config-path=/home/<YOUR HOME DIR>/python-kafka-streaming-twitter/deply/conf/twitter-parser.properties
```

Open browser and go to local yarn resource manager: http://localhost:8088/cluster to check status

### 8.Create a consumer to view the stream.
```
/home/deploy/kafka/bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic tweets-parsed
```

### NEXT
Use live streams to analyze trends, geographical data modelling and implement deep learning algos


### LICENSE
See license.txt

Feel free to reach me if you have any queries.





