package socialnetwork.util;

import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.io.File;

public class Config {

    // Kafka config
    public final static String LOCAL_ZOOKEEPER_HOST = "localhost:2181";
    public final static String LOCAL_KAFKA_BROKER = "localhost:9092";
    public final static String KAFKA_GROUP = "test-consumer-group";
    public static final int numKafkaPartitions = 1;
    public final static String allActivitiesTopic = numKafkaPartitions > 1 ? "all-multiple" : "all-single";
    // producer
    public static final boolean use1KFiles = true;
    public static final String Likes_1K = "data/1k-users-cleaned/streams/likes_event_stream.csv";
    public static final String Comments_1K = "data/1k-users-cleaned/streams/comment_event_stream.csv";
    public static final String Posts_1K = "data/1k-users-cleaned/streams/post_event_stream.csv";
    public static final String Likes_10K = "data/10k-users-cleaned/streams/likes_event_stream.csv";
    public static final String Comments_10K = "data/10k-users-cleaned/streams/comment_event_stream.csv";
    public static final String Posts_10K = "data/10k-users-cleaned/streams/post_event_stream.csv";
    public static final boolean useSpeedupFactor = false;
    public static final int speedupFactor = 900000;
    public static final boolean produceInOrder = false;
    public final static Time outOfOrdernessBound = Time.minutes(5);
    public static String[] getStreamInputFiles() {
        return use1KFiles ?
                new String[] {Comments_1K, Likes_1K, Posts_1K} : new String[] {Comments_10K, Likes_10K, Posts_10K};
    }
    public static String[] getStreamPrefixs() {
        return new String[] {"C|", "L|", "P|"};
    }

    // Flink config
    public final static boolean useLocalEnvironmentWithWebUI = true;  // setting to true sets up the dashboard at http://localhost:8081/
    public final static int flinkParallelism = 4;
    public final static OutputTag<String> mappingOutputTag = new OutputTag<String>("mapping-output"){};
    public final static String mappingOutputFilename = "log/actual_mappings.txt";
    public final static OutputTag<String> errorOutputTag = new OutputTag<String>("error-output"){};
    public final static String errorOutputFilename = "log/errors.txt";
    public final static String resolvedStreamOutputFilename = "log/resolved_stream.txt";


    // Task 1 TODO tba

    // Task 2 TODO change the values
    public final static Integer[] eigenUserIds = new Integer[] {10000, 10001};
    public final static String path_person_knows_person = System.getProperty("user.home") + "/repo/eth-dspa-2019/project/data/test/person_knows_person.csv";
    public final static String path_person_hasInterest_tag = System.getProperty("user.home") + "/repo/eth-dspa-2019/project/data/test/person_hasInterest_tag.csv";
    public final static String path_person_isLocatedIn_place = System.getProperty("user.home") + "/repo/eth-dspa-2019/project/data/test/person_isLocatedIn_place.csv";
    public final static String path_person_studyAt_organisation = System.getProperty("user.home") + "/repo/eth-dspa-2019/project/data/test/person_studyAt_organisation.csv";
    public final static String path_person_workAt_organisation = System.getProperty("user.home") + "/repo/eth-dspa-2019/project/data/test/person_workAt_organisation.csv";
    public final static Double staticWeight = 0.3;

}
