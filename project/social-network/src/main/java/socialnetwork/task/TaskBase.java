package socialnetwork.task;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import socialnetwork.util.Activity;

public abstract class TaskBase <V> {
    public abstract void buildPipeline(StreamExecutionEnvironment env, DataStream<V> inputStream);
}
