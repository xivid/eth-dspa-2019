package socialnetwork;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import socialnetwork.util.Activity;
import socialnetwork.util.Config;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;

import static socialnetwork.util.Config.*;

public class Producer {
    private final KafkaProducer<String, String> producer;

    public Producer() {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, LOCAL_KAFKA_BROKER);
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producer = new KafkaProducer<>(props);
    }

    private void produceProportional(boolean inOrder) {
//        try {
//            FileParser parser = new FileParser();
//            // data structure that will contain all data from files
//            final TreeMap<Long, ArrayList<Activity>> data =
//                    inOrder ?
//                            parser.generateInOrderData(Config.getCleanedFiles()) :
//                            parser.generateOutOfOrderData(Config.getCleanedFiles());
//            System.out.println("All data input has been processed\n");
//            System.out.println(String.format("Producing %s proportionally to Kafka topic %s...\n", inOrder ? "in order" : "out of order", Config.allActivitiesTopic));
//
//            long first = 0L;
//            long numberOfSentRecords = 0;
//            while (!data.isEmpty()) {
//                Map.Entry<Long, ArrayList<Tuple3<Data_Type, Long, String>>> entry = data.pollFirstEntry();
//                ArrayList<Tuple3<Data_Type,Long,String>> list = entry.getValue();
//                long date_ms = 0;
//
//                for(Tuple3<Data_Type, Long, String> t : list) {
//                    date_ms = t.f1;
//                    if(first == 0) {
//                        first = t.f1;
//                    }
//
//                    producer.send(new ProducerRecord<>(Config.TOPIC, null,
//                            t.f0.get_val() + SPLIT_CHAR + t.f1 + SPLIT_CHAR + t.f2)).get();
//
//                    numberOfSentRecords++;
//                }
//
//                if(!data.isEmpty()) {
//                    long next_time = data.firstKey();
//                    final long sleepDuration = (next_time - date_ms) / speedupFactor;
//                    if(sleepDuration <= 0) {
//                        continue;
//                    }
//                    Thread.sleep(sleepDuration);
//                }
//            }
//            System.out.println(String.format("Sent %d records to kafka", numberOfSentRecords));
//
//            // Semd tpmbstone messages
//            for(long i = Long.MAX_VALUE - numKafkaPartitions; i < Long.MAX_VALUE; i++) {
//                String tombstone = Post.createFromId(i).toString();
//                producer.send(new ProducerRecord<>(Config.TOPIC, null,
//                        Data_Type.TOMBSTONE.get_val() + SPLIT_CHAR + Long.MAX_VALUE + SPLIT_CHAR + tombstone)).get();
//            }
//            System.out.println("Finished producing to Kafka");
//        } catch (FileNotFoundException e) {
//            System.out.println("File not found");
//            e.printStackTrace();
//        } catch (IOException e) {
//            System.out.println("Cannot read file");
//            e.printStackTrace();
//        } catch (ParseException e) {
//            System.out.println("Parse exception");
//            e.printStackTrace();
//        } catch (InterruptedException e) {
//            System.out.println("Interrupted exception");
//            e.printStackTrace();
//        } catch (ExecutionException e) {
//            System.out.println("Execution exception");
//            e.printStackTrace();
//        }
    }

    private void produce(boolean inOrder) {
//        try {
//            // data structure that will contain all data from files
//            FileParser parser = new FileParser();
//            final TreeMap<Long, ArrayList<Tuple3<Data_Type, Long, String>>> data =
//                    inOrder ?
//                            parser.generateInOrderData(Config.getCleanedFiles()) :
//                            parser.generateOutOfOrderData(Config.getCleanedFiles());
//            final String topic_to_publish = numKafkaPartitions > 1 ? "all-multiple" : "all-single";
//
//            System.out.println("All data input has been read in\n");
//            System.out.println(String.format("Producing %s to Kafka topic %s...\n", inOrder ? "in order" : "out of order", Config.TOPIC));
//
//            long numberOfSentRecords = 0;
//            while (!data.isEmpty()) {
//                Map.Entry<Long, ArrayList<Tuple3<Data_Type, Long, String>>> entry = data.pollFirstEntry();
//                ArrayList<Tuple3<Data_Type, Long, String>> list = entry.getValue();
//                for(Tuple3<Data_Type, Long, String> t : list) {
//                    producer.send(new ProducerRecord<>(Config.TOPIC, null,
//                            t.f0.get_val() + SPLIT_CHAR + t.f1 + SPLIT_CHAR + t.f2)).get();
//                    numberOfSentRecords++;
//                }
//            }
//            System.out.println(String.format("Sent %d records to kafka", numberOfSentRecords));
//
//            // Semd tombstone messages
//            for(long i = Long.MAX_VALUE - numKafkaPartitions; i < Long.MAX_VALUE; i++) {
//                String tombstone = Post.createFromId(i).toString();
//                producer.send(new ProducerRecord<>(Config.TOPIC, null,
//                        Data_Type.TOMBSTONE.get_val() + SPLIT_CHAR + Long.MAX_VALUE + SPLIT_CHAR + tombstone)).get();
//            }
//            System.out.println("Finished producing to Kafka");
//        } catch (FileNotFoundException e) {
//            System.out.println("File not found");
//            e.printStackTrace();
//        } catch (IOException e) {
//            System.out.println("Cannot read file");
//            e.printStackTrace();
//        } catch (ParseException e) {
//            System.out.println("Parse exception");
//            e.printStackTrace();
//        }  catch (InterruptedException e) {
//            System.out.println("Interrupted exception");
//            e.printStackTrace();
//        } catch (ExecutionException e) {
//            System.out.println("Execution exception");
//            e.printStackTrace();
//        }
    }

    public static void main(String[] args) {
        Producer kafka = new Producer();
        if(Config.useSpeedupFactor) {
            kafka.produceProportional(Config.produceInOrder);
        } else {
            kafka.produce(Config.produceInOrder);
        }
    }
}

