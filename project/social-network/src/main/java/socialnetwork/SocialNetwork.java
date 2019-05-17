package socialnetwork;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import socialnetwork.task.activepost.ActivePostStatistician;
import socialnetwork.task.postidresolution.PostIdResolver;
import socialnetwork.task.recommendation.FriendRecommender;
import socialnetwork.util.Activity;
import socialnetwork.util.Config;

import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SocialNetwork {
    final static Logger logger = LoggerFactory.getLogger("SocialNetwork");

    public static void main(String[] args) throws Exception {
        logger.info("Social Network Started");

        /* set up the streaming execution environment */
        final StreamExecutionEnvironment env;
        if (Config.useLocalEnvironmentWithWebUI) {
            Configuration config = new Configuration();
            config.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
            env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
        } else {
            env = StreamExecutionEnvironment.getExecutionEnvironment();
        }
        env.setParallelism(Config.parallelism);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        /* Ingest activities from Kafka */
        DataStream<Activity> allActivitiesStream = getAllActivitiesStream(env);

        /* Build postId-resolved input stream */
        PostIdResolver postIdResolver = new PostIdResolver();
        SingleOutputStreamOperator<Activity> postIdResolvedAllActivitiesStream = postIdResolver.buildPipeline(env, allActivitiesStream);

        /* Perform tasks */
        ActivePostStatistician task1 = new ActivePostStatistician();
        task1.buildPipeline(env, allActivitiesStream);

//        FriendRecommender task2 = new FriendRecommender();
//        task2.buildPipeline(env, allActivitiesStream);
//        task2.buildTestPipeline(env);

        // TODO task 3

        env.execute("Social Network");
    }

    public static DataStream<Activity> getAllActivitiesStream(StreamExecutionEnvironment env) {
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("zookeeper.connect", Config.LOCAL_ZOOKEEPER_HOST);
        kafkaProps.setProperty("bootstrap.servers", Config.LOCAL_KAFKA_BROKER);
        kafkaProps.setProperty("group.id", Config.KAFKA_GROUP);
        // always read the Kafka topic from the start
        kafkaProps.setProperty("auto.offset.reset", "earliest");
        return env
            .addSource(new FlinkKafkaConsumer011<>("all-activities", new Activity.Deserializer(), kafkaProps))
            .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Activity>(Config.outOfOrdernessBound) {
                public long extractTimestamp(Activity a) {
                    return a.getEventTimestamp();
                }
            });
    }

}