package socialnetwork.util;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.TimeZone;

public class Helpers {

    /**
     * For each input message, emit it with the accompanying timestamp
     * @param <T> Type of the message
     */
    public static class GetMessageWithTimestamp<T> extends ProcessFunction<T, Tuple2<LocalDateTime, T>> implements Serializable {

        public void processElement(T in, Context context, Collector<Tuple2<LocalDateTime, T>> collector) throws Exception {
            collector.collect(new Tuple2<>(
                    LocalDateTime.ofInstant(Instant.ofEpochMilli(context.timestamp()), TimeZone.getTimeZone("GMT+0").toZoneId()),
                    in)
            );
        }

    }
}
