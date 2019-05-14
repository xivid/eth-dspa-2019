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

import java.io.*;
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
-------------------------------
Test stream: data/task2.txt
-------------------------------
Post,1,10000,2019-05-01 08:00:00  // TODO change time span to have meaningful output for 4hr/1hr
Post,2,10001,2019-05-01 08:30:22
Comment,1,10000,2019-05-01 08:35:40
Comment,1,10010,2019-05-01 09:00:09
Comment,2,10000,2019-05-01 10:00:10
Like,1,10001,2019-05-01 13:59:59
Comment,1,10001,2019-05-01 15:22:33
Comment,2,10010,2019-05-02 03:00:50
Like,1,10000,2019-05-03 12:00:35
Like,2,10001,2019-05-04 19:00:33

------------------------------------
Test table: person_knows_person.csv
------------------------------------
Person.id|Person.id
10000|10001
10001|10010
*/
public class Task2 {

    private static ArrayList<HashSet<Long>> getExistingFriendships(Long[] eigenUserIds, String csv_path) throws IOException {
        // use hashmap first for easy lookup
        HashMap<Long, HashSet<Long>> friendSets = new HashMap<>();
        for (Long userId : eigenUserIds) {
            friendSets.putIfAbsent(userId, new HashSet<>());
        }

        // store concerned relationships
        InputStream csv_stream = new FileInputStream(csv_path);
        BufferedReader reader = new BufferedReader(new InputStreamReader(csv_stream));
        // skip the header of the csv
        reader.lines().skip(1).forEach(line -> {
            String[] splits = line.split("\\|");
            Long knower = Long.valueOf(splits[0]);
            Long knowee = Long.valueOf(splits[1]);
            if (friendSets.containsKey(knower)) {
                friendSets.get(knower).add(knowee);
            }
        });
        reader.close();

        // put into arraylist
        ArrayList<HashSet<Long>> retList = new ArrayList<>();
        for (Long userId : eigenUserIds) {
            retList.add(friendSets.get(userId));
        }
        return retList;
    }

    private static ArrayList<HashMap<Long, Integer>> getStaticSimilarities(String path_person_hasInterest_tag,
                                                                           String path_person_isLocatedIn_place,
                                                                           String path_person_studyAt_organisation,
                                                                           String path_person_workAt_organisation) {

        return null;
    }

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// config
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		final Time outOfOrdernessBound = Time.minutes(0); // TODO test out-of-orderness
		final Long[] eigenUserIds = new Long[] {10000L, 10001L};  // TODO get from config
        final String path_person_knows_person = System.getProperty("user.home") + "/repo/eth-dspa-2019/project/data/person_knows_person.csv";
        final String path_person_hasInterest_tag = System.getProperty("user.home") + "/repo/eth-dspa-2019/project/data/person_hasInterest_tag.csv";
        final String path_person_isLocatedIn_place = System.getProperty("user.home") + "/repo/eth-dspa-2019/project/data/person_isLocatedIn_place.csv";
        final String path_person_studyAt_organisation = System.getProperty("user.home") + "/repo/eth-dspa-2019/project/data/person_studyAt_organisation.csv";
        final String path_person_workAt_organisation = System.getProperty("user.home") + "/repo/eth-dspa-2019/project/data/person_workAt_organisation.csv";
		final ArrayList<HashSet<Long>> alreadyFriends = getExistingFriendships(eigenUserIds, path_person_knows_person);  // TODO read from real tables
        final ArrayList<HashMap<Long, Integer>>
                staticSimilarities = getStaticSimilarities(path_person_hasInterest_tag,
                                                           path_person_isLocatedIn_place,
                                                           path_person_studyAt_organisation,
                                                           path_person_workAt_organisation);  // TODO read from real tables

        // get input stream
		DataStream<Activity> input =  // TODO get unioned, postId-resolved stream
			env.readTextFile(System.getProperty("user.home") + "/repo/eth-dspa-2019/project/data/task2.txt")
			   .map(Activity::new)
			   .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Activity>(outOfOrdernessBound) {
				   public long extractTimestamp(Activity a) {
					   return a.timestamp;
				   }
			   });
        input.print().setParallelism(1);

        // get per-post similarities with a keyed sliding window
		DataStream<ArrayList<HashMap<Long, Integer>>> similaritiesPerPost = input
			.keyBy(activity -> activity.postId)
			.window(SlidingEventTimeWindows.of(Time.days(2), Time.days(1))) // TODO (Time.hours(4), Time.hours(1)))
            .aggregate(new CountActivitiesPerUser(), new GetUserSimilarities(eigenUserIds, alreadyFriends));
		// similaritiesPerPost.print().setParallelism(1);

		// Use another window to sum up the per-post similarities
		DataStream<ArrayList<ArrayList<Long>>> recommendations = similaritiesPerPost
            .windowAll(TumblingEventTimeWindows.of(Time.days(1))) // TODO (Time.hours(4), Time.hours(1)))
            .aggregate(new SimilarityAggregate(eigenUserIds), new GetTopFiveRecommendations(eigenUserIds));
		// recommendations.print().setParallelism(1);

		// execute program
		env.execute("Task 2 Friend Recommendation");
	}

	private static class CountActivitiesPerUser
            implements AggregateFunction<Activity, HashMap<Long, Integer>, HashMap<Long, Integer>> {

        @Override
        public HashMap<Long, Integer> createAccumulator() {
            return new HashMap<>();  // userId -> count
        }

        @Override
        public HashMap<Long, Integer> add(Activity value, HashMap<Long, Integer> accumulator) {
            accumulator.merge(value.userId, 1, Integer::sum);
            return accumulator;
        }

        @Override
        public HashMap<Long, Integer> getResult(HashMap<Long, Integer> accumulator) {
            return accumulator;
        }

        @Override
        public HashMap<Long, Integer> merge(HashMap<Long, Integer> r1, HashMap<Long, Integer> r2) {
            for (HashMap.Entry<Long, Integer> elem : r1.entrySet()) {
                r2.merge(elem.getKey(), elem.getValue(), Integer::sum);
            }
            return r2;
        }
    }

	public static class GetUserSimilarities
			extends ProcessWindowFunction<HashMap<Long, Integer>, ArrayList<HashMap<Long, Integer>>, Long, TimeWindow> {

		private Long[] eigenUserIds;
		private ArrayList<HashSet<Long>> alreadyFriends;

		GetUserSimilarities(Long[] eigenUserIds, ArrayList<HashSet<Long>> alreadyFriends) {
		    this.eigenUserIds = eigenUserIds;
		    this.alreadyFriends = alreadyFriends;
		}

        String pretty(TimeWindow w) {
            LocalDateTime start = LocalDateTime.ofInstant(Instant.ofEpochMilli(w.getStart()),
                    TimeZone.getTimeZone("GMT+0").toZoneId());
            LocalDateTime end = LocalDateTime.ofInstant(Instant.ofEpochMilli(w.getEnd()),
                    TimeZone.getTimeZone("GMT+0").toZoneId());
            return "{" + start + "-" + end + "}";
        }

		@Override
		public void process(Long postId, Context context, Iterable<HashMap<Long, Integer>> input, Collector<ArrayList<HashMap<Long, Integer>>> out) {
			// init similarity matrix
			ArrayList<HashMap<Long, Integer>> similarities = new ArrayList<>();  // similarities[eigenUsers][allUsers] -> similarity
			for (int i = 0; i < eigenUserIds.length; ++i) {
				similarities.add(new HashMap<>());
			}

			// count activities for every user
			HashMap<Long, Integer> counts = input.iterator().next();  // userId -> count

			// calculate similarity
			for (int i = 0; i < eigenUserIds.length; ++i) {
				Integer eigenCount = counts.get(eigenUserIds[i]);
				if (eigenCount != null) {
					for (HashMap.Entry<Long, Integer> elem : counts.entrySet()) {
						Long userId = elem.getKey();
						if (!userId.equals(eigenUserIds[i]) && !alreadyFriends.get(i).contains(userId)) {
						    // eliminate the already friend users here, so that size of similarities can be reduced (less communication overhead)
                            Integer userCount = elem.getValue();
                            similarities.get(i).put(userId, eigenCount * userCount);
                        }
					}
				}
			}

			System.out.println("PostId: " + postId + ", Window: " + pretty(context.window()) + ", similarities: " + similarities);
			out.collect(similarities);
		}
	}

	private static class SimilarityAggregate
			implements AggregateFunction<ArrayList<HashMap<Long, Integer>>, ArrayList<HashMap<Long, Integer>>, ArrayList<HashMap<Long, Integer>>> {

		private Long[] eigenUserIds;

		SimilarityAggregate(Long[] eigenUserIds) { this.eigenUserIds = eigenUserIds; }

		@Override
		public ArrayList<HashMap<Long, Integer>> createAccumulator() {
			ArrayList<HashMap<Long, Integer>> accu = new ArrayList<>();  // similarities[eigenUsers][allUsers]
			for (int i = 0; i < eigenUserIds.length; ++i) {
				accu.add(new HashMap<>());
			}
			return accu;
		}

		@Override
		public ArrayList<HashMap<Long, Integer>> add(ArrayList<HashMap<Long, Integer>> value, ArrayList<HashMap<Long, Integer>> accumulator) {
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
			for (int i = 0; i < eigenUserIds.length; i++) {
				ArrayList<Long> recommendationsPerUser = new ArrayList<>();
				int c = 0;
				for (HashMap.Entry<Long, Integer> elem : similarities.get(i).entrySet()) { // TODO first normalize the per-eigen-user-similarities to [0,1] via dividing by max
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
