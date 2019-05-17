package socialnetwork.task;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import socialnetwork.util.Activity;

public abstract class TaskBase <IN, OUT> {
    public abstract DataStream<OUT> buildPipeline(StreamExecutionEnvironment env, DataStream<IN> inputStream);
}
