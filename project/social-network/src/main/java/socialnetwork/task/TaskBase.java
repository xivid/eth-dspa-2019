package socialnetwork.task;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public abstract class TaskBase <IN> {
    public abstract void buildPipeline(StreamExecutionEnvironment env, DataStream<IN> inputStream);
}
