package socialnetwork.util;

import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

public class Config {
    // Useful constants
    public static final long SECOND = 1000;
    public static final long MINUTE = 60 * SECOND;
    public static final long HOUR = 60 * MINUTE;

    // Kafka config
    public final static String LOCAL_ZOOKEEPER_HOST = "localhost:2181";
    public final static String LOCAL_KAFKA_BROKER = "localhost:9092";
    public final static String KAFKA_GROUP = "test-consumer-group";
    public static final int numKafkaPartitions = 4;
    public final static String allActivitiesTopic = numKafkaPartitions > 1 ? "all-multiple" : "all-single";
    // producer
    public static final boolean use1KFiles = true;
    public static final String Likes_1K = "data/1k-users-cleaned/streams/likes_event_stream.csv";
    public static final String Comments_1K = "data/1k-users-cleaned/streams/comment_event_stream.csv";
    public static final String Posts_1K = "data/1k-users-cleaned/streams/post_event_stream.csv";
    public static final String Likes_10K = "data/10k-users-cleaned/streams/likes_event_stream.csv";
    public static final String Comments_10K = "data/10k-users-cleaned/streams/comment_event_stream.csv";
    public static final String Posts_10K = "data/10k-users-cleaned/streams/post_event_stream.csv";

    private static final String LIKES_1K_RAW = "data/1k-users-raw/streams/likes_event_stream.csv";
    private static final String COMMENTS_1K_RAW = "data/1k-users-raw/streams/comment_event_stream.csv";
    private static final String POSTS_1K_RAW = "data/1k-users-raw/streams/post_event_stream.csv";
    private static final String LIKES_10K_RAW = "data/10k-users-raw/streams/likes_event_stream.csv";
    private static final String COMMENTS_10K_RAW = "data/10k-users-raw/streams/comment_event_stream.csv";
    private static final String POSTS_10K_RAW = "data/10k-users-raw/streams/post_event_stream.csv";

    // if useSpeedupFactor == false: produce at max speed
    // if useSpeedupFactor == true && speedupFactor == 1: produce at event real time speed
    public static final boolean useSpeedupFactor = false;
    public static final int speedupFactor = 900000;
    public static final boolean produceInOrder = false;
    public final static Time outOfOrdernessBound = Time.minutes(5);

    public static String[] getStreamCleanedInputFiles() {
        return use1KFiles ?
                new String[] {Comments_1K, Likes_1K, Posts_1K} :
                new String[] {Comments_10K, Likes_10K, Posts_10K};
    }

    public static String[] getStreamRawInputFiles() {
        return use1KFiles ?
                new String[] {COMMENTS_1K_RAW, LIKES_1K_RAW, POSTS_1K_RAW} :
                new String[] {COMMENTS_10K_RAW, LIKES_10K_RAW, POSTS_10K_RAW};
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


    // Task 1
    public final static String lateCommentsOutputFilename = "log/late-comments.txt";
    public final static String commentCountsOutputFilename = "log/comment-counts.txt";
    public final static String lateRepliesOutputFilename = "log/late-replies.txt";
    public final static String replyCountsOutputFilename = "log/reply-counts.txt";
    public final static String userCountsOutputFilename = "log/user-counts.txt";

    // Task 2
    public final static Integer[] eigenUserIds = new Integer[] {38, 534, 941, 347, 303, 495, 884, 540, 336, 405};
    public final static String person_knows_person_1K = "data/1k-users-raw/tables/person_knows_person.csv";
    public final static String person_hasInterest_tag_1K = "data/1k-users-raw/tables/person_hasInterest_tag.csv";
    public final static String person_isLocatedIn_place_1K = "data/1k-users-raw/tables/person_isLocatedIn_place.csv";
    public final static String person_studyAt_organisation_1K = "data/1k-users-raw/tables/person_studyAt_organisation.csv";
    public final static String person_workAt_organisation_1K = "data/1k-users-raw/tables/person_workAt_organisation.csv";
    public final static String person_knows_person_10K = "data/10k-users-raw/tables/person_knows_person.csv";
    public final static String person_hasInterest_tag_10K = "data/10k-users-raw/tables/person_hasInterest_tag.csv";
    public final static String person_isLocatedIn_place_10K = "data/10k-users-raw/tables/person_isLocatedIn_place.csv";
    public final static String person_studyAt_organisation_10K = "data/10k-users-raw/tables/person_studyAt_organisation.csv";
    public final static String person_workAt_organisation_10K = "data/10k-users-raw/tables/person_workAt_organisation.csv";
    public final static Double staticWeight = 0.3;

    // Task 3
    public final static String anomaliesOutputFilename = "log/anomalies.txt";

}
