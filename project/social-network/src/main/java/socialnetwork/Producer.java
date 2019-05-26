package socialnetwork;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import socialnetwork.util.Activity;
import socialnetwork.util.Config;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static socialnetwork.util.Config.*;

public class Producer {

    public static TreeMap<Long, List<Activity>> readCleanedStreams(boolean inOrder, int lateness) throws IOException {
        TreeMap<Long, List<Activity>> map = new TreeMap<>();
        final String[] files = Config.getStreamCleanedInputFiles();
        final String[] prefixs = Config.getStreamPrefixs();
        Random random = new Random();

        for (int i = 0; i < files.length; i++) {
            final BufferedReader reader =
                    new BufferedReader(new InputStreamReader(new FileInputStream(files[i])));
            reader.readLine(); // avoid header
            String line;
            while ((line = reader.readLine()) != null) {
                Activity record = Activity.fromString(prefixs[i] + line);
                long outputTimestamp = record.getCreationTimestamp();
                if (!inOrder) {
                    outputTimestamp += random.nextInt(lateness);
                }
                if (!map.containsKey(outputTimestamp)) {
                    map.put(outputTimestamp, new ArrayList<>());
                }
                map.get(outputTimestamp).add(record);
            }
        }
        System.out.println("Finished reading all activities.\n");
        return map;
    }

    private static void produceToKafka(KafkaProducer<String, String> producer, TreeMap<Long, List<Activity>> allActivitiesByTimestamp) {
        try {
            System.out.println(String.format("Producing %s %sto Kafka topic %s...\n", Config.produceInOrder ? "in order" : "out of order", Config.useSpeedupFactor ? "proportionally " : "", Config.allActivitiesTopic));
            long numberOfSentRecords = 0;

            Map.Entry<Long, List<Activity>> entry = allActivitiesByTimestamp.pollFirstEntry();
            while (entry != null) {
                long this_time = entry.getKey();
                List<Activity> list = entry.getValue();

                // send activities assigned to this key (timestamp)
                for(Activity t : list) {
                    producer.send(new ProducerRecord<>(Config.allActivitiesTopic, null, t.toString())).get();
                }
                numberOfSentRecords += list.size();

                // if map has next key
                entry = allActivitiesByTimestamp.pollFirstEntry();
                if(entry != null && Config.useSpeedupFactor) {
                    long next_time = entry.getKey();
                    final long sleepDuration = (next_time - this_time) / speedupFactor;
                    if(sleepDuration <= 0) {
                        continue;
                    }
                    Thread.sleep(sleepDuration);
                }
            }
            System.out.println(String.format("Sent %d records to kafka", numberOfSentRecords));

            // Send tombstone messages
            for(int i = 0; i < numKafkaPartitions; i++) {
                Activity.Tombstone tombstone = new Activity.Tombstone(i, "9999-12-31T23:59:59Z");
                producer.send(new ProducerRecord<>(Config.allActivitiesTopic, null, tombstone.toString())).get();
            }
            System.out.println("Finished producing to Kafka");
        } catch (InterruptedException e) {
            System.out.println("Interrupted exception");
            e.printStackTrace();
        } catch (ExecutionException e) {
            System.out.println("Execution exception");
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, LOCAL_KAFKA_BROKER);
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        final TreeMap<Long, List<Activity>> allActivitiesByTimestamp = readCleanedStreams(produceInOrder, (int) outOfOrdernessBound.toMilliseconds());
        produceToKafka(producer, allActivitiesByTimestamp);
    }
}

