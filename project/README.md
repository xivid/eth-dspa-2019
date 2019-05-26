# Social Network

## How to run
1. Start zookeeper and kafka, create a topic `all-single` or `all-multiple` depending on the number of partitions you want from Kafka.
2. Launch a memcached instance at port 11211: `memcached -p 11211`.
3. Change the necessary settings in config: `src/main/java/socialnetwork/util/Config.java`.
4. Start the flink program, the main class is `src/main/java/socialnetwork/SocialNetwork.java`.
5. The output of each task will be written to the files specified in the config.

## Data Processing
The project works with cleaned data. One can use the tools in `src/main/java/socialnetwork/cleaning` to clean the data. See the readme there.

## Tasks and result validation
The implementations of all three tasks are under `src/main/java/socialnetwork/task`. For the purpose of validation, the programs `Task*Evaluator` under `src/main/java/socialnetwork/validation` can be run to generate the expected output by batch processing.

## Authors
Jack Clark, Zhifei Yang
