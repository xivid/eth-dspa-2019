/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package wikiedits;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource;
import org.apache.flink.util.Preconditions;

import java.util.Optional;

import static org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducerBase.getPropertiesFromBrokerList;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class FlinkKafkaProducer {

    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        /*
         * Here, you can start creating your execution plan for Flink.
         *
         * Start with getting some data from the environment, like
         * 	env.readTextFile(textPath);
         *
         * then, transform the resulting DataStream<String> using operations
         * like
         * 	.filter()
         * 	.flatMap()
         * 	.join()
         * 	.coGroup()
         *
         * and many more.
         * Have a look at the programming guide for the Java API:
         *
         * http://flink.apache.org/docs/latest/apis/streaming/index.html
         *
         */
        DataStream<WikipediaEditEvent> edits = env.addSource(new WikipediaEditsSource());

//		DataStream<String> stream = edits.map((WikipediaEditEvent e) -> new Tuple2<>(e.getUser(), e.getByteDiff()).toString());
        DataStream<String> stream = edits.map(WikipediaEditEvent::toString);
        FlinkKafkaProducer011<String> myProducer = new FlinkKafkaProducer011<String>(
                "wiki-edits", // target topic
                new SimpleStringSchema(),  // serialization schema
                getPropertiesFromBrokerList("localhost:9092"), // broker list
                Optional.of(new CustomPartitioner<>()));  // round robin partitioner
        stream.addSink(myProducer);

        DataStream<Tuple2<String, Integer>> result = edits
                // project the event user and the diff
                .map(new MapFunction<WikipediaEditEvent, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(WikipediaEditEvent event) {
                        return new Tuple2<>(
                                event.getUser(), event.getByteDiff());
                    }
                })
                // group by user
                .keyBy(0)
                // aggregate changes per user
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> e1, Tuple2<String, Integer> e2) {
                        return new Tuple2<>(e1.f0, e1.f1 + e2.f1);
                    }
                })
                // filter out negative byte changes
                .filter(new FilterFunction<Tuple2<String, Integer>>() {
                    @Override
                    public boolean filter(Tuple2<String, Integer> e) throws Exception {
                        return e.f1 >= 0;
                    }
                });
//		result.print();

        // execute program
        env.execute("Flink Streaming Java API Skeleton");
    }

    static class CustomPartitioner<T> extends FlinkKafkaPartitioner<T> {
        private int next = 0;

        @Override
        public int partition(T record, byte[] key, byte[] value, String targetTopic, int[] partitions) {
            Preconditions.checkArgument(partitions != null && partitions.length > 0, "Partitions of the target topic is empty.");
            this.next = (this.next + 1) % partitions.length;
            return partitions[this.next];
        }
    }
}
