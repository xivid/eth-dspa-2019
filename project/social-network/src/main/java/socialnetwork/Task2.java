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

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Arrays.asList;

/*
Test stream: data/task2.txt

Post,1,10000,2019-05-01 08:00:00
Post,2,10001,2019-05-01 08:30:22
Comment,1,10000,2019-05-01 08:35:40
Comment,1,10010,2019-05-01 09:00:09
Comment,2,10000,2019-05-01 10:00:10
Like,1,10001,2019-05-01 13:59:59
Comment,1,10001,2019-05-01 15:22:33
Comment,2,10010,2019-05-02 03:00:50
Like,1,10000,2019-05-03 12:00:35
Like,2,10001,2019-05-04 19:00:33
 */
public class Task2 {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// config
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		final Time outOfOrdernessBound = Time.minutes(0);
		final Long[] eigenUserIds = new Long[] {10000L, 10001L};  // TODO get from config
		final ArrayList<HashMap<Long, Boolean>> isFriend = new ArrayList<>(); // TODO read from person_knows_person.csv

		// get input
		DataStream<Activity> input =  // TODO get unioned, postId-resolved stream
			env.readTextFile(System.getProperty("user.home") + "/repo/eth-dspa-2019/project/data/task2.txt")
			   .map(Activity::new)
			   .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Activity>(outOfOrdernessBound) {
				   public long extractTimestamp(Activity a) {
					   return a.timestamp;
				   }
			   });

        input.print().setParallelism(1);

		DataStream<ArrayList<HashMap<Long, Integer>>> similaritiesPerPost = input
			.keyBy(activity -> activity.postId)
			.window(SlidingEventTimeWindows.of(Time.days(2), Time.days(1))) //(Time.hours(4), Time.hours(1)))
			.process(new GetUserSimilarities(eigenUserIds));  // TODO: change this to aggregate

		similaritiesPerPost.print().setParallelism(1);

		// Use consecutive windowed operations
		// https://ci.apache.org/projects/flink/flink-docs-release-1.8/dev/stream/operators/windows.html#consecutive-windowed-operations
		DataStream<ArrayList<ArrayList<Long>>> recommendations = similaritiesPerPost
				.windowAll(TumblingEventTimeWindows.of(Time.days(1))) //(Time.hours(4), Time.hours(1)))
//                .process(new AggregateAndGetTopFiveRecommendations(eigenUserIds));

				.aggregate(new SimilarityAggregate(eigenUserIds), new GetTopFiveRecommendations(eigenUserIds));  // TODO fix: why are there always two accumulators?

//		recommendations.print().setParallelism(1);

		// execute program
		env.execute("Task 2 Friend Recommendation");
	}

	public static class GetUserSimilarities
			extends ProcessWindowFunction<Activity, ArrayList<HashMap<Long, Integer>>, Long, TimeWindow> {

		private Long[] eigenUserIds;

		public GetUserSimilarities(Long[] eigenUserIds) { this.eigenUserIds = eigenUserIds; }

        String pretty(TimeWindow w) {
            LocalDateTime start = LocalDateTime.ofInstant(Instant.ofEpochMilli(w.getStart()),
                    TimeZone.getTimeZone("GMT+0").toZoneId());
            LocalDateTime end = LocalDateTime.ofInstant(Instant.ofEpochMilli(w.getEnd()),
                    TimeZone.getTimeZone("GMT+0").toZoneId());
            return "{" + start + "-" + end + "}";
        }

		@Override
		public void process(Long postId, Context context, Iterable<Activity> input, Collector<ArrayList<HashMap<Long, Integer>>> out) {
			// init similarity matrix
			ArrayList<HashMap<Long, Integer>> similarities = new ArrayList<>();  // similarities[eigenUsers][allUsers]
			for (int i = 0; i < eigenUserIds.length; ++i) {
				HashMap<Long, Integer> map = new HashMap<>();
				similarities.add(map);
			}

			// count activities for every user (TODO: this can be done by `aggregate`)
			HashMap<Long, Integer> counts = new HashMap<>();  // userId -> num of activities on this post
			for (Activity activity: input) {
				counts.merge(activity.userId, 1, Integer::sum);
			}

			// calculate similarity
			for (int i = 0; i < eigenUserIds.length; ++i) {
				Integer eigenCount = counts.get(eigenUserIds[i]);
				if (eigenCount != null) {
					for (HashMap.Entry<Long, Integer> elem : counts.entrySet()) {
						Long userId = elem.getKey();
						Integer userCount = elem.getValue();
						similarities.get(i).put(userId, eigenCount * userCount);  // if self or friend: put -1
					}
				}
			}

			System.out.println("PostId: " + postId + ", Window: " + pretty(context.window()) + ", similarities: " + similarities);
			out.collect(similarities);
		}
	}

    public static class AggregateAndGetTopFiveRecommendations
            extends ProcessAllWindowFunction<ArrayList<HashMap<Long, Integer>>, ArrayList<ArrayList<Long>>, TimeWindow> {

        private Long[] eigenUserIds;

        public AggregateAndGetTopFiveRecommendations(Long[] eigenUserIds) { this.eigenUserIds = eigenUserIds; }

        String pretty(TimeWindow w) {
            LocalDateTime start = LocalDateTime.ofInstant(Instant.ofEpochMilli(w.getStart()),
                    TimeZone.getTimeZone("GMT+0").toZoneId());
            LocalDateTime end = LocalDateTime.ofInstant(Instant.ofEpochMilli(w.getEnd()),
                    TimeZone.getTimeZone("GMT+0").toZoneId());
            return "{" + start + "-" + end + "}";
        }

        @Override
        public void process(Context context, Iterable<ArrayList<HashMap<Long, Integer>>> input, Collector<ArrayList<ArrayList<Long>>> out) {

//            input.iterator().hasNext()
            // get global similarities
            ArrayList<HashMap<Long, Integer>> similarities = null;  // similarities[eigenUsers][allUsers]
			for (ArrayList<HashMap<Long, Integer>> local : input) {
			    if (similarities == null) {
			        // take the vector of the first post
			        similarities = local;
                    System.out.println("Window: " + pretty(context.window()) + ", init: " + similarities);
                } else {
			        // add the vector of another post
			        for (int i = 0; i < local.size(); ++i) {
                        for (HashMap.Entry<Long, Integer> elem : local.get(i).entrySet()) {
                            similarities.get(i).merge(elem.getKey(), elem.getValue(), Integer::sum);
				        }
                    }
                    System.out.println("Window: " + pretty(context.window()) + ", add: " + local + ", accu: " + similarities);
                }
            }

            System.out.println("Window: " + pretty(context.window()) + ", similarities: " + similarities);

			assert similarities != null;

			// get recommendations
            ArrayList<ArrayList<Long>> recommendations = new ArrayList<>();
            for (int i = 0; i < similarities.size(); i++) {
                ArrayList<Long> recommendationsPerUser = new ArrayList<>();
                int c = 0;
                for (HashMap.Entry<Long, Integer> elem : similarities.get(i).entrySet()) {
                    recommendationsPerUser.add(elem.getKey());  // TODO select 5 with max similarity, and add static measure
                    if (++c == 5) break;
                }
                recommendations.add(recommendationsPerUser);
                System.out.println("Window: " + pretty(context.window()) + ", recommend for " + eigenUserIds[i] + ": " + recommendationsPerUser);
            }

            out.collect(recommendations);
        }
    }


	private static class SimilarityAggregate
			implements AggregateFunction<ArrayList<HashMap<Long, Integer>>, ArrayList<HashMap<Long, Integer>>, ArrayList<HashMap<Long, Integer>>> {

		private Long[] eigenUserIds;

		public SimilarityAggregate(Long[] eigenUserIds) { this.eigenUserIds = eigenUserIds; }

		@Override
		public ArrayList<HashMap<Long, Integer>> createAccumulator() {
			ArrayList<HashMap<Long, Integer>> accu = new ArrayList<>();  // similarities[eigenUsers][allUsers]
			for (int i = 0; i < eigenUserIds.length; ++i) {
				accu.add(new HashMap<>());
			}
			System.out.println("Created accumulator " + System.identityHashCode(accu));
			return accu;
		}

		@Override
		public ArrayList<HashMap<Long, Integer>> add(ArrayList<HashMap<Long, Integer>> value, ArrayList<HashMap<Long, Integer>> accumulator) {
			System.out.println("add value: " + value + ", accu " + System.identityHashCode(accumulator) +": " + accumulator);
			assert value.size() == accumulator.size();
			for (int i = 0; i < accumulator.size(); ++i) {
				for (HashMap.Entry<Long, Integer> elem : value.get(i).entrySet()) {
					accumulator.get(i).merge(elem.getKey(), elem.getValue(), Integer::sum);
				}
			}
			return accumulator;
		}

		@Override
		public ArrayList<HashMap<Long, Integer>> getResult(ArrayList<HashMap<Long, Integer>> accumulator) {
			return accumulator;
		}

		@Override
		public ArrayList<HashMap<Long, Integer>> merge(ArrayList<HashMap<Long, Integer>> r1, ArrayList<HashMap<Long, Integer>> r2) {
			System.out.println("merge r1: " + r1 + ", r2: " + r2);
			return add(r1, r2);
		}
	}

	private static class GetTopFiveRecommendations
			extends ProcessAllWindowFunction<ArrayList<HashMap<Long, Integer>>, ArrayList<ArrayList<Long>>, TimeWindow> {

		private Long[] eigenUserIds;

		GetTopFiveRecommendations(Long[] eigenUserIds) { this.eigenUserIds = eigenUserIds; }

        String pretty(TimeWindow w) {
            LocalDateTime start = LocalDateTime.ofInstant(Instant.ofEpochMilli(w.getStart()),
                    TimeZone.getTimeZone("GMT+0").toZoneId());
            LocalDateTime end = LocalDateTime.ofInstant(Instant.ofEpochMilli(w.getEnd()),
                    TimeZone.getTimeZone("GMT+0").toZoneId());
            return "{" + start + "-" + end + "}";
        }

		public void process(Context context,
							Iterable<ArrayList<HashMap<Long, Integer>>> aggregations,
							Collector<ArrayList<ArrayList<Long>>> out) {

			ArrayList<HashMap<Long, Integer>> similarities = aggregations.iterator().next();
			ArrayList<ArrayList<Long>> recommendations = new ArrayList<>();

			System.out.println("Window: " + pretty(context.window()) + ", similarities: " + similarities);
			for (int i = 0; i < similarities.size(); i++) {
				ArrayList<Long> recommendationsPerUser = new ArrayList<>();
				int c = 0;
				for (HashMap.Entry<Long, Integer> elem : similarities.get(i).entrySet()) {
					recommendationsPerUser.add(elem.getKey());  // TODO select 5 with max similarity, and add static measure
					if (++c == 5) break;
				}
				recommendations.add(recommendationsPerUser);
				System.out.println("Window: " + pretty(context.window()) + ", recommend for " + eigenUserIds[i] + ": " + recommendationsPerUser);
			}

			out.collect(recommendations);
		}
	}

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
			this.timestamp = eventTime.atZone(ZoneId.of("GMT+0")).toInstant().toEpochMilli();
		}

		@Override
		public String toString() {
			return "(type: " + type + ", postId: " + postId + ", userId: " + userId
					+ ", eventTime: " + eventTime + ", timestamp: " + timestamp + ")";
		}
	}

}
