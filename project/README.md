# Social Network

## How to run
1. Start zookeeper and kafka, create a topic `all-single` or `all-multiple` depending on the number of partitions you want from Kafka.
2. Launch a memcached instance at port 11211: `memcached -p 11211`.
3. Change the necessary settings in config: `social-network/src/main/java/socialnetwork/util/Config.java`. See the section below for explanations on the configs.
4. Start the flink program, the main class is `social-network/src/main/java/socialnetwork/SocialNetwork.java`.
5. The output of each task will be written to the files specified in the config.

## Configuration
Important parameters in `Config.java` include:
- `useSpeedupFactor` and `speedupFactor`: controls the input stream producing speed. (false, *) produces everything as fast as possible. (true, 1) produces in the original (real time) speed. (true, 100) tries to produce 100 seconds of events in 1 second (of course, this is limited by the processor).
- `produceInOrder` and `outOfOrdernessBound`: controls whether the input streams should be produced out-of-order, and the maximum lateness to produce.
- `flinkParallelism`: max parallelism of the flink tasks.
- `numKafkaPartitions`: how many kafka partitions should the producer produce to and the flink program consume from? This should be no more than `flinkParallelism`, because according to our design, every partition has a final Tombstone message to tell the flink program to terminate. If a flink task reads from more than one partition, since we can't get the exact number of partitions the task reads from, it has to stop as soon as one Tombstone is seen, which could prevent reading remaining messages from other partitions. If you don't want this behaviour, remove lines 77~81 in `Producer.java`.
- `eigenUserIds`: the 10 users to recommend friends for in task 2.
- `staticWeight`: weight of static similarities in task 2.

## Tasks and result validation
The implementations of all three tasks are under `social-network/src/main/java/socialnetwork/task`. For the purpose of validation, the programs `Task*Evaluator` under `social-network/src/main/java/socialnetwork/validation` can be run to generate the expected output by batch processing.

## Data Preprocessing
The project works with cleaned data. One can run the tools in `social-network/src/main/java/socialnetwork/cleaning` to clean the data. Set `use1KFiles` in config accordingly to clean the 1K or 10K streams.

## Authors
Jack Clark, Zhifei Yang
