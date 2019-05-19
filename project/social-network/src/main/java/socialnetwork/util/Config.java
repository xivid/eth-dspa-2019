package socialnetwork.util;

import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

public class Config {

    // Kafka config
    public final static String LOCAL_ZOOKEEPER_HOST = "localhost:2181";
    public final static String LOCAL_KAFKA_BROKER = "localhost:9092";
    public final static String KAFKA_GROUP = "test-consumer-group";
    public static final int numKafkaPartitions = 1;
    public final static String allActivitiesTopic = numKafkaPartitions > 1 ? "all-multiple" : "all-single";

    // producer
    private static final boolean use1KFiles = true;
    public static final boolean useSpeedupFactor = false;
    public static final int speedupFactor = 900000;
    public static final boolean produceInOrder = false;
    public final static Time outOfOrdernessBound = Time.minutes(5);

    // Flink config
    public final static boolean useLocalEnvironmentWithWebUI = true;
    public final static int parallelism = 4;
    public final static OutputTag<String> mappingOutputTag = new OutputTag<String>("mapping-output"){};
    public final static String mappingOutputFilename = "actual_mappings.txt";
    public final static OutputTag<String> errorOutputTag = new OutputTag<String>("error-output"){};
    public final static String errorOutputFilename = "errors.txt";
    public final static String resolvedStreamOutputFilename = "resolved_stream.txt";


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
