package socialnetwork;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import socialnetwork.task.activepost.ActivePostStatistician;
import socialnetwork.task.anomalydetection.AnomalousUserDetector;
import socialnetwork.task.postidresolution.PostIdResolver;
import socialnetwork.task.recommendation.FriendRecommender;
import socialnetwork.util.Activity;
import socialnetwork.util.Config;

import java.util.Properties;

public class SocialNetwork {
    final static Logger logger = LoggerFactory.getLogger("SocialNetwork");

    public static void main(String[] args) throws Exception {
        logger.info("Social Network Started");

        logger.info("Setting up the stream execution environment");
        final StreamExecutionEnvironment env = setupEnvironment();

        logger.info("Building Dataflow: Ingest activities from Kafka");
        DataStream<Activity> allActivitiesStream = getAllActivitiesStream(env);

        logger.info("Building Dataflow: Resolve postId");
        PostIdResolver postIdResolver = new PostIdResolver();
        postIdResolver.buildPipeline(env, allActivitiesStream);
        SingleOutputStreamOperator<Activity> postIdResolvedAllActivitiesStream = postIdResolver.getResolvedStream();

//        logger.info("Building Dataflow: Task 1 Active Post Statistics");
//        ActivePostStatistician task1 = new ActivePostStatistician();
//        task1.buildPipeline(env, postIdResolvedAllActivitiesStream);
//
//        logger.info("Building Dataflow: Task 2 Friend Recommendation");
//        FriendRecommender task2 = new FriendRecommender();
//        task2.buildPipeline(env, postIdResolvedAllActivitiesStream);

        logger.info("Building Dataflow: Task 3 Unusual Activity Detection");
        AnomalousUserDetector task3 = new AnomalousUserDetector();
        task3.buildPipeline(env, postIdResolvedAllActivitiesStream);

        env.execute("Social Network");
    }

    public static StreamExecutionEnvironment setupEnvironment() {
        StreamExecutionEnvironment env;
        if (Config.useLocalEnvironmentWithWebUI) {
            Configuration config = new Configuration();
            config.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
            env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
        } else {
            env = StreamExecutionEnvironment.getExecutionEnvironment();
        }
        env.setParallelism(Config.flinkParallelism);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        return env;
    }

    public static DataStream<Activity> getAllActivitiesStream(StreamExecutionEnvironment env) {
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("zookeeper.connect", Config.LOCAL_ZOOKEEPER_HOST);
        kafkaProps.setProperty("bootstrap.servers", Config.LOCAL_KAFKA_BROKER);
        kafkaProps.setProperty("group.id", Config.KAFKA_GROUP);
        // always read the Kafka topic from the start
        kafkaProps.setProperty("auto.offset.reset", "earliest");
        FlinkKafkaConsumer011<Activity> consumer =
                new FlinkKafkaConsumer011<>(Config.allActivitiesTopic, new Activity.Deserializer(), kafkaProps);
        consumer.setStartFromEarliest();
        consumer.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Activity>(Config.outOfOrdernessBound) {
            @Override
            public long extractTimestamp(Activity a) {
                return a.getCreationTimestamp();
            }
        });
        return env
            .addSource(consumer)
            .setParallelism(Integer.min(Config.numKafkaPartitions, Config.flinkParallelism));
    }

}
