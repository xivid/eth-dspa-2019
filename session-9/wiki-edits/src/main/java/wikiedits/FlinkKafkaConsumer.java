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

import java.util.HashMap;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
import org.apache.flink.util.Collector;

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
public class FlinkKafkaConsumer {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// Set a fixed delay restart strategy with a maximum of 5 restart attempts
		// and a 1s interval between retries
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, 1000));

		Properties kafkaProps = new Properties();
		kafkaProps.setProperty("zookeeper.connect", "localhost:2181");
		kafkaProps.setProperty("bootstrap.servers", "localhost:9092");
		kafkaProps.setProperty("group.id", "test-consumer-group");
		kafkaProps.setProperty("enable.auto.commit", "false");
		// always read the Kafka topic from the start
		kafkaProps.setProperty("auto.offset.reset", "earliest");
		DataStream<Tuple2<String, Integer>> edits = env
				.addSource(new FlinkKafkaConsumer011<>("wiki-edits",
						new CustomDeserializationSchema(), kafkaProps))
				.setParallelism(1)
				.map(new MapFunction<WikipediaEditEvent, Tuple2<String, Integer>>() {
					@Override
					public Tuple2<String, Integer> map(WikipediaEditEvent event) {
						return new Tuple2<>(
								event.getUser(), event.getByteDiff());
					}
				});

		DataStream<Tuple2<String, Integer>> results = edits
			// group by user
			.keyBy(0)
			.flatMap(new ComputeDiffs());
		results.print().setParallelism(1);

		// execute program
		env.execute("Flink Streaming Java API Skeleton");
	}

	// Keep track of user byte diffs in a HashMap
	public static final class ComputeDiffs extends RichFlatMapFunction<
				Tuple2<String, Integer>, Tuple2<String, Integer>> {

		// user -> diffs
		private HashMap<String, Integer> diffs;

		@Override
		public void open(Configuration parameters) throws Exception {
			diffs = new HashMap<>();
		}

		@Override
		public void flatMap(Tuple2<String, Integer> in,
							Collector<Tuple2<String, Integer>> out) throws Exception {
			String user = in.f0;
			int diff = in.f1;
			if (diffs.containsKey(user)) {
				// Update existing HashMap value
				diffs.put(user, diffs.get(user) + diff);
			}
			else {
				// Insert new user in HashMap
				diffs.put(user, diff);
			}
			out.collect(new Tuple2<String, Integer>(user, diffs.get(user)));
		}
	}

	static class CustomDeserializationSchema extends AbstractDeserializationSchema<WikipediaEditEvent> {
		@Override

		public WikipediaEditEvent deserialize(byte[] message) {
			// format:
			// "WikipediaEditEvent{timestamp=" + this.timestamp + ", channel='" + this.channel + '\'' + ", title='" + this.title + '\'' + ", diffUrl='" + this.diffUrl + '\'' + ", user='" + this.user + '\'' + ", byteDiff=" + this.byteDiff + ", summary='" + this.summary + '\'' + ", flags=" + this.flags + '}';
			// example:
			// WikipediaEditEvent{timestamp=1552870599316, channel='#en.wikipedia', title='Talk:Shooting of Trayvon Martin', diffUrl='https://en.wikipedi=888128575', user='Psalm84', byteDiff=-2, summary='/* Proposed split */ minor word changes', flags=33}

			String s = new String(message);
			Pattern p = Pattern.compile("WikipediaEditEvent\\{timestamp=(.*), channel='(.*)', title='(.*)', diffUrl='(.*)', user='(.*)', byteDiff=(.*), summary='(.*)', flags=(.*)}");
			Matcher m = p.matcher(s);
			if (m.find()) {
				int flags = Integer.valueOf(m.group(8));
				boolean isMinor = (flags & 1) != 0;
				boolean isNew = (flags & 2) != 0;
				boolean isUnpatrolled = (flags & 4) != 0;
				boolean isBotEdit = (flags & 8) != 0;
				boolean isSpecial = (flags & 16) != 0;
				boolean isTalk = (flags & 32) != 0;

				return new WikipediaEditEvent(Long.valueOf(m.group(1)), m.group(2), m.group(3), m.group(4), m.group(5), Integer.valueOf(m.group(6)), m.group(7), isMinor, isNew, isUnpatrolled, isBotEdit, isSpecial, isTalk);
			} else {
				throw new RuntimeException("error in parsing!");
			}
		}
	}
}
