# Social Network

## How to run
1. Start zookeeper: `./bin/zookeeper-server-start.sh config/zookeeper.properties`. 
2. Start Kafka: `./bin/kafka-server-start.sh ./config/server.properties`.
3. Create a topic called `all-multiple` in Kafka: `bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 3 --topic all-multiple`.
4. Launch a memcached instance at port 11211: `memcached -p 11211 -m 8192 -t 1`.
5. Once you have cloned our repository, run `cd project/social-network` to change to root working directory. 
6. You will need a cleaned version of the raw input files. To do this, run `mkdir data && cp -r PATH_TO_FILES/1k-users-sorted/* data/1k-users-raw && mkdir data/10k-users-raw && cp -r PATH_TO_FILES/10k-users-sorted/* data/10k-users-raw` changing PATH_TO_FILES to the location where the 1k-users and 10k-users folders are located on your system.
7. To convert the raw files into cleaned files, run the StreamsCleaner in the cleaning package. To run this on the 1k files, set the `use1KFiles = true` in the Config file, and vice versa for the 10k files.

You should now be ready to start configuring and running the Producer and Flink application.

8. The producer can be configured with a number of options. The important ones are `produceInOrder` and the corresponding `outOfOrdernessBound`, and `useSpeedupFactor` and the corresponding `speedupFactor`, and finally `numKafkaPartitions`. To test the application with the requirements outlined in the project, set `produceInOrder = false`, `outOfOrdernessBound = Time.minutes(30)` (or some other value > 0), `useSpeedupFactor = true` and `speedupFactor = 604800` (or some other higher value). It should be noted that running the application with a speedupFactor increases the time it takes for it to run, so if you want to quickly test something, we recommend not using a speedupFactor and letting Flink from Kafka as quickly as possible. You should set `numKafkaPartitions` to the number of Kafka partitions that you configured when creating the `all-multiple` topic, for the command above, this would be 3.

9. To configure the flink application, you need to additionally set the `flinkParallelism`. We recommend setting this to half the cores on your machine, as setting it higher risks starving memcached of cpu time, which can cause requests to time out. In a real production environment, this is not a problem as you could either put memcached on another machine, allocate more memcached instances, or configure it with more threads, but for simplicity we choose to just ensure it gets enough cpu time by reducing the contention. To enable the flink web UI to run whilst running flink from Intellij, ensure `useLocalEnvironmentWithWebUI` is set to true in the Config file.

With the above configuration options set, everything should be ready to run.

10. Run the Producer. You should see output that matches your configuration options e.g. "Producing out of order to Kafka topic all-multiple...".

11. Now start the flink application. The main class is the SocialNetwork class. As we produce tombstone values at the end of our streams, flink will automatically stop running when it has processed all of the data.

## Tasks and result validation
The implementations of all three tasks are under `social-network/src/main/java/socialnetwork/task`. For the purpose of validation, the programs `Task*Evaluator` under `social-network/src/main/java/socialnetwork/validation` can be run to generate the expected output by batch processing.

## Data Preprocessing
The project works with cleaned data. One can run the tools in `social-network/src/main/java/socialnetwork/cleaning` to clean the data. Set `use1KFiles` in config accordingly to clean the 1K or 10K streams.


- `eigenUserIds`: the 10 users to recommend friends for in task 2.
- `staticWeight`: weight of static similarities in task 2.

## Authors
Jack Clark, Zhifei Yang
