package socialnetwork.task.postidresolution;

import net.spy.memcached.MemcachedClient;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import socialnetwork.task.TaskBase;
import socialnetwork.util.Activity;
import socialnetwork.util.Config;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.Future;

public class PostIdResolver extends TaskBase<Activity, Activity> {
    final static Logger logger = LoggerFactory.getLogger("SocialNetwork");

    @Override
    public SingleOutputStreamOperator<Activity> buildPipeline(StreamExecutionEnvironment env, DataStream<Activity> inputStream) {

        SingleOutputStreamOperator<Activity> stream = inputStream
                .rebalance()
                .map(new WriteMessageIdToMemcached())
                .keyBy(Activity::getKey)  // TODO maybe need checking with Reply.class.isInstance(this) ?
                .process(new MappingResolver());

        stream.writeAsText(Config.resolvedStreamOutputFilename, FileSystem.WriteMode.OVERWRITE).setParallelism(1);

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
        public Activity map(Activity activity) throws Exception {
            if(activity.isCommentOrReply()) {
                Activity.Comment comment = (Activity.Comment) activity;

                Integer childId = comment.getSelfId();
                Integer parentId = comment.getParentId();
                String parentPrefix = activity.isReply() ? "r_" : "p_";  // is parent Comment or Post?

                Future<Boolean> setRequest = mc.set("r_" + childId, 0, parentPrefix + parentId);
                while(!setRequest.isDone());
                if(!setRequest.get()) {
                    logger.error("set request {} -> {} returned false", "r_" + childId, parentPrefix + parentId);
                }
            }
            return activity;
        }
    }


    public static class MappingResolver extends KeyedProcessFunction<Integer, Activity, Activity> implements Serializable {
        private MemcachedClient mc;
        private Map<Integer, Activity.Reply> map;
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
        public void processElement(Activity activity,
                                   Context context,
                                   Collector<Activity> collector) throws Exception {
            switch (activity.getType()) {
                case Tombstone:
                    logger.error("TOMBSTONE received at task {}", getRuntimeContext().getIndexOfThisSubtask());
                    break;

                case Post:
                case Like:
                    collector.collect(activity);
                    break;

                case Comment: {
                    Activity.Comment comment = (Activity.Comment) activity;
                    String postId = "p_" + comment.getParentId();
                    String childId = String.valueOf(comment.getSelfId());

                    Future<Boolean> setRequest = mc.set(childId, 0, postId);
                    while (!setRequest.isDone()) ;
                    if (!setRequest.get()) {
                        logger.error("set request {} -> {} returned false", childId, postId);
                    }

                    context.output(Config.mappingOutputTag, childId + " -> " + postId);
                    break;
                }

                case Reply: {
                    // Initially, I should try to resolve the mapping
                    Activity.Reply reply = (Activity.Reply) activity;
                    String currentKey = "r_" + reply.getParentId();
                    String prevKey;

                    do {
                        prevKey = currentKey;
                        currentKey = (String) mc.get(currentKey);
                    } while(currentKey != null && !currentKey.startsWith("p_"));

                    if(currentKey == null) { // mapping unresolved
                        // Save any progress I have made into the k/v store
                        String childId = "r_" + reply.getSelfId();
                        Future<Boolean> setRequest = mc.set(childId, 0, prevKey);
                        while(!setRequest.isDone());
                        if(!setRequest.get()) {
                            logger.error("set request {} -> {} returned false", childId, prevKey);
                        }

                        // then register a timer for my timestamp + MAX_DELAY in the future
                        map.put(context.getCurrentKey(), reply);
                        context.timerService().registerEventTimeTimer(
                                reply.getCreationTimestamp() + Config.outOfOrdernessBound.toMilliseconds());
                    }
                    else if(currentKey.startsWith("p_")) { // solved the mapping
                        resolve(context, reply, currentKey);
                    }

                    break;
                }

                default:
                    logger.error("Got to end of switch trying to resolve post id mappings");
            }

            collector.collect(activity);
        }

        @Override
        public void onTimer(long timestamp,
                            OnTimerContext context,
                            Collector<Activity> collector) throws Exception {
            // Try to resolve the mapping. This should always succeed,
            // except when the comment/reply id is part of the blacklisted ids.
            Activity.Reply reply = map.get(context.getCurrentKey());

            String currentKey = "r_" + reply.getParentId();
            do {
                currentKey = (String) mc.get(currentKey);
            } while(currentKey != null && !currentKey.startsWith("p_"));

            if(currentKey == null) { // mapping unresolved
                // TODO: Handle the case that the mapping is not resolved because of the blacklist
                logger.error("Mapping could not be resolved. This should never be the case for test data.");
                context.output(errorTag, "Mapping could not be resolved." +
                        " This should never be the case for test data.\n\t Reply: " +
                        reply.toString());
            }
            else if(currentKey.startsWith("p_")) { // solved the mapping
                resolve(context, reply, currentKey);
                collector.collect(reply);
            }
            logger.error("ERROR: Got to end of onTimer trying to resolve post id mappings");
        }

        private void resolve(Context context, Activity.Reply reply, String post) throws Exception {
            String childId = "r_" + reply.getSelfId();
            Future<Boolean> setRequest = mc.set(childId, 0, post);
            while(!setRequest.isDone());
            if(!setRequest.get()) {
                logger.error("set request {} -> {} returned false", childId, post);
            }
            reply.setPostId(Integer.valueOf(post.substring(2)));
            context.output(Config.mappingOutputTag, childId + " -> " + post);
        }
    }

}
