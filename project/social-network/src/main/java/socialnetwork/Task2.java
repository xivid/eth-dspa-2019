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

package socialnetwork;

import akka.stream.impl.fusing.Collect;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.operators.Keys;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;

public class Task2 {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		DataStream<Activity> input =
			env.readTextFile("/Users/zhifei/repo/eth-dspa-2019/project/data/task2.txt")
			   .map(Activity::new)
			   .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Activity>(Time.minutes(30)) {
				   public long extractTimestamp(Activity a) {
					   return a.timestamp;
				   }
			   });

		input.keyBy(new KeySelector<Activity, Tuple2<Long, Long>>() {
						@Override
						public Tuple2<Long, Long> getKey(Activity value) throws Exception {
							return Tuple2.of(value.postId, value.userId);
						}
				    })
			 .window(SlidingEventTimeWindows.of(Time.days(2), Time.days(1))) //(Time.hours(4), Time.hours(1)))
//			 .fold("\n\n", new FoldFunction<Activity, String>() {
//					public String fold(String acc, Activity value) {
//						return acc + value;
//					}
//				})
			 .aggregate(new CountAggregate(), new CountProcessWindowFunction())
			 .print();

		// execute program
		env.execute("Task 2 Friend Recommendation");
	}

	private static class CountAggregate
			implements AggregateFunction<Activity, Long, Long> {
		@Override
		public Long createAccumulator() {
			return 0L;
		}

		@Override
		public Long add(Activity a, Long accumulator) {
			return accumulator + 1L;
		}

		@Override
		public Long getResult(Long accumulator) {
			return accumulator;
		}

		@Override
		public Long merge(Long a, Long b) {
			return a + b;
		}
	}


	private static class CountProcessWindowFunction
			extends ProcessWindowFunction<Long, Tuple3<Long, Long, Long>, Tuple2<Long, Long>, TimeWindow> {

		public void process(Tuple2<Long, Long> key,
							Context context,
							Iterable<Long> counts,
							Collector<Tuple3<Long, Long, Long>> out) {
			Long count = counts.iterator().next();
			out.collect(new Tuple3<>(key.f0, key.f1, count));
		}
	}
//
//	private static class CountProcessWindowFunction
//			extends ProcessWindowFunction<Long, Tuple3<Long, Long, Long>, Tuple2<Long, Long>, TimeWindow> {
//
//		public void process(Tuple2<Long, Long> keys,
//							Context context,
//							Iterable<Long> counts,
//							Collector<Tuple3<Long, Long, Long>> out) {
//			Long count = counts.iterator().next();
//			out.collect(new Tuple3<>(keys.f0, keys.f1, count));
//		}
//	}

	// Data type for Activities
	public static class Activity {

		public enum ActivityType {
			Post,
			Comment,
			Like,
			Others;

			static ActivityType fromString(String s) {
				if (s.equals("Post")) return Post;
				if (s.equals("Comment")) return Comment;
				if (s.equals("Like")) return Like;
				return Others;
			}
		};

		public ActivityType type;
		public Long postId;
		public Long userId;
		public LocalDateTime eventTime;
		public Long timestamp;

		public Activity(String line) {
			String[] splits = line.split(",");
			this.type = ActivityType.fromString(splits[0]);
			this.postId = Long.valueOf(splits[1]);
			this.userId = Long.valueOf(splits[2]);
			this.eventTime = LocalDateTime.from(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").parse(splits[3]));
			this.timestamp = eventTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
		}

		@Override
		public String toString() {
			return "(type: " + type + ", postId: " + postId + ", userId: " + userId
					+ ", eventTime: " + eventTime + ", timestamp: " + timestamp + ")";
		}
	}

//	public static void main(String[] args) throws Exception {
//
//		// the port to connect to
//		final int port;
//		try {
//			final ParameterTool params = ParameterTool.fromArgs(args);
//			port = params.getInt("port");
//		} catch (Exception e) {
//			System.err.println("No port specified. Please run 'SocketWindowWordCount --port <port>'");
//			return;
//		}
//
//		// get the execution environment
//		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//		// get input data by connecting to the socket
//		DataStream<String> text = env.socketTextStream("localhost", port, "\n");
//
//		// parse the data, group it, window it, and aggregate the counts
//		DataStream<WordWithCount> windowCounts = text
//				.flatMap(new FlatMapFunction<String, WordWithCount>() {
//					@Override
//					public void flatMap(String value, Collector<WordWithCount> out) {
//						for (String word : value.split("\\s")) {
//							out.collect(new WordWithCount(word, 1L));
//						}
//					}
//				})
//				.keyBy("word")
//				.timeWindow(Time.seconds(5), Time.seconds(3))
//				.reduce(new ReduceFunction<WordWithCount>() {
//					@Override
//					public WordWithCount reduce(WordWithCount a, WordWithCount b) {
//						return new WordWithCount(a.word, a.count + b.count);
//					}
//				});
//
//		// print the results with a single thread, rather than in parallel
//		windowCounts.print().setParallelism(1);
//
//		env.execute("Task 2 Friend Recommendation");
//	}
//
//	// Data type for words with count
//	public static class WordWithCount {
//
//		public String word;
//		public long count;
//
//		public WordWithCount() {}
//
//		public WordWithCount(String word, long count) {
//			this.word = word;
//			this.count = count;
//		}
//
//		@Override
//		public String toString() {
//			return word + " : " + count;
//		}
//	}
}
