package socialnetwork.util;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.TimeZone;

public class Helpers {

    private static final Logger logger = LoggerFactory.getLogger("SocialNetwork");
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

    public static BufferedReader getFileReader(String path) {
        try {
            File f = new File(path);
            return new BufferedReader(new InputStreamReader(new FileInputStream(f)));
        } catch (FileNotFoundException e) {
            System.out.println("Error: cannot find file \"" + path + "\"");
            System.out.println("Current path: "+ System.getProperty("user.dir"));
        }
        return null;
    }

    public static BufferedWriter getFileWriter(String path) {
        try {
            File f = new File(path);
            if (f.getParentFile() != null) {
                f.getParentFile().mkdirs();
            }
            f.createNewFile();
            return new BufferedWriter(new OutputStreamWriter(new FileOutputStream(f, false)));
        } catch (FileNotFoundException e) {
            System.out.println("Error: cannot find file \"" + path + "\"");
            System.out.println("Current path: "+ System.getProperty("user.dir"));
        } catch (IOException e) {
            System.out.println("IOException when creating file at location: " + path);
        }
        return  null;
    }
}
