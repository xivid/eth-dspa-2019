package socialnetwork;

import net.spy.memcached.MemcachedClient;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import socialnetwork.task.Task1;
import socialnetwork.task.Task2;
import socialnetwork.util.Activity;
import socialnetwork.util.Config;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.Future;

public class SocialNetwork {
    public static void main(String[] args) throws Exception {
        // TODO add log4j2

        /* set up the streaming execution environment */
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(Config.parallelism);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        /* TODO Ingest activities from Kafka */
        DataStream<Activity> allActivitiesStream = getAllActivitiesStream(env);

        /* TODO Build postId-resolved input stream */
        SingleOutputStreamOperator<Activity> postIdResolvedAllActivitiesStream = getPostIdResolvedAllActivitiesStream(allActivitiesStream);

        /* Perform tasks */
        Task1 task1 = new Task1();
        task1.buildPipeline(env, allActivitiesStream);

        Task2 task2 = new Task2();
        task2.buildPipeline(env, allActivitiesStream);

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
                    return a.timestamp;
                }
            });
    }

    public static SingleOutputStreamOperator<Activity> getPostIdResolvedAllActivitiesStream(DataStream<Activity> allActivitiesStream) {
        SingleOutputStreamOperator<Activity> stream = allActivitiesStream
                .rebalance()
                .map(new WriteMessageIdToMemcached())
                .keyBy(val -> val.getId())
                .process(new PostIdResolver());

        stream
                .getSideOutput(Config.mappingOutputTag)
                .writeAsText(Config.mappingOutputFilename, FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);

        stream
                .getSideOutput(Config.errorOutputTag)
                .writeAsText(Config.errorOutputFilename, FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);

        return stream;
    }


    public static class WriteMessageIdToMemcached extends RichMapFunction<Activity, Activity> implements Serializable {
        private MemcachedClient mc;

        @Override
        public void open(Configuration configuration) {
            try {
                mc = new MemcachedClient(new InetSocketAddress("localhost", 11211));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void close() {
            mc.shutdown();
        }

        @Override
        public Activity map(Activity value) throws Exception {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            sdf.setTimeZone(TimeZone.getTimeZone("GMT"));

            if(value.isCommentOrReply()) {
                // TODO
//                String childId = Message.id(value.f0, value.f2);
//                String parentId;
//                Comment comment = (Comment) value.f2;
//                if(value.f0 == COMMENT) {
//                    long postId = comment.getReply_to_postId();
//                    parentId = "p_" + postId;
//                } else {
//                    long commentId = comment.getReply_to_commentId();
//                    parentId = "r_" + commentId;
//                }
//                Future<Boolean> setRequest = mc.set(childId, 0, parentId);
//                while(!setRequest.isDone());
//                if(!setRequest.get()) {
//                    System.out.println("set request returned false");
//                }
            }
            return value;
        }
    }


    public static class PostIdResolver extends KeyedProcessFunction<Long, Activity, Activity> implements Serializable {
        private MemcachedClient mc;
        private Map<Long, Activity> map;
        private final OutputTag<String> errorTag = Config.errorOutputTag;

        @Override
        public void open(Configuration configuration) {
            try {
                mc = new MemcachedClient(new InetSocketAddress("localhost", 11211));
                map = new HashMap<>();
            } catch (IOException e) {
                System.out.println(e);
            }
        }

        @Override
        public void close() {
            mc.shutdown();
        }

        @Override
        public void processElement(Activity value,
                                   Context context,
                                   Collector<Activity> collector) throws Exception {
//            if(value.f0 == TOMBSTONE) {
//                System.out.println(String.format("TOMBSTONE received at task %d", getRuntimeContext().getIndexOfThisSubtask()));
//                return;
//            }
//
//            // posts and likes are solved by default
//            if(value.f0 == POST || value.f0 == LIKE) {
//                collector.collect(new Tuple3<>(Message.get_postid(value.f0, value.f2), value.f0, value.f2));
//                return;
//            }
//
//            if(value.f0 == COMMENT) {
//                long postId = Message.get_postid(value.f0, value.f2);
//                String postIdVal = "p_" + postId;
//                String childId = Message.id(value.f0, value.f2);
//                Future<Boolean> setRequest = mc.set(childId, 0, postIdVal);
//                while(!setRequest.isDone());
//                if(!setRequest.get()) {
//                    System.out.println("set request returned false");
//                }
//                collector.collect(new Tuple3<>(postId, value.f0, value.f2));
//
//                String mapping = childId + " -> " + postIdVal;
//                context.output(Config.mappingOutputTag, mapping);
//                return;
//            }
//
//            // Initially, I should try to resolve the mapping
//            String currentKey = "r_" + Message.get_postid(value.f0, value.f2);
//            String prevKey;
//
//            do {
//                prevKey = currentKey;
//                currentKey = (String) mc.get(currentKey);
//            } while(currentKey != null && !currentKey.startsWith("p_"));
//
//            if(currentKey == null) { // mapping unresolved
//                // If I can't I save any progress I have made into the k/v store
//                String childId = Message.id(value.f0, value.f2);
//                Future<Boolean> setRequest = mc.set(childId, 0, prevKey);
//                while(!setRequest.isDone());
//                if(!setRequest.get()) {
//                    System.out.println("set request returned false");
//                }
//
//                // then register a timer for my timestamp + MAX_DELAY in the future
//                map.put(context.getCurrentKey(), value);
//                context.timerService().registerEventTimeTimer(
//                        value.f1 + Config.MAX_DELAY.toMilliseconds());
//                return;
//            }
//
//            if(currentKey.startsWith("p_")) { // solved the mapping
//                String childId = Message.id(value.f0, value.f2);
//                Future<Boolean> setRequest = mc.set(childId, 0, currentKey);
//                while(!setRequest.isDone());
//                if(!setRequest.get()) {
//                    System.out.println("set request returned false");
//                }
//                long postId = Long.parseLong(currentKey.substring(2));
//                collector.collect(new Tuple3<>(postId, value.f0, value.f2));
//
//                String mapping = childId + " -> " + currentKey;
//                context.output(Config.mappingOutputTag, mapping);
//                return;
//            }
//
//            System.out.println("ERROR: Got to end of KeyedProcessFunction trying to resolve post id mappings");
        }

        @Override
        public void onTimer(long timestamp,
                            OnTimerContext context,
                            Collector<Activity> collector) throws Exception {
//            // Try to resolve the mapping. This should always succeed,
//            // except when the comment/reply id is part of the blacklisted ids.
//            Tuple3<Data_Type, Long, Message> value = map.get(context.getCurrentKey());
//            if(value == null) {
//                System.out.println("ERROR: tuple retrieved from map should never be null");
//                context.output(errorTag, "ERROR: tuple retrieved from map should never be null");
//                return;
//            }
//
//            String currentKey = "r_" + Message.get_postid(value.f0, value.f2);
//
//            do {
//                currentKey = (String) mc.get(currentKey);
//            } while(currentKey != null && !currentKey.startsWith("p_"));
//
//            if(currentKey == null) { // mapping unresolved
//                // TODO: Handle the case that the mapping is not resolved because of the blacklist
//                System.out.println("ERROR: Mapping could not be resolved. This should never be the case for test data.");
//                context.output(errorTag, String.format("ERROR: Mapping could not be resolved." +
//                        " This should never be the case for test data.\n" +
//                        "TYPE = %s\n" +
//                        "FAKE_T = %s" +
//                        "\nMESSAGE = %s", value.f0, value.f1, value.f2));
//                return;
//            }
//
//            if(currentKey.startsWith("p_")) { // solved the mapping
//                String childId = Message.id(value.f0, value.f2);
//                Future<Boolean> setRequest = mc.set(childId, 0, currentKey);
//                while(!setRequest.isDone());
//                if(!setRequest.get()) {
//                    System.out.println("set request returned false");
//                }
//                long postId = Long.parseLong(currentKey.substring(2));
//                collector.collect(new Tuple3<>(postId, value.f0, value.f2));
//
//                String mapping = childId + " -> " + currentKey;
//                context.output(Config.mappingOutputTag, mapping);
//                return;
//            }
//            System.out.println("ERROR: Got to end of KeyedProcessFunction trying to resolve post id mappings");
        }
    }

}