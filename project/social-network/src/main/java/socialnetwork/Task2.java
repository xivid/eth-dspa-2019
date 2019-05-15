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
import java.time.format.DateTimeFormatter;
import java.util.*;

public class Task2 {

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
		final ArrayList<HashSet<Long>> alreadyKnows = getExistingFriendships(eigenUserIds, path_person_knows_person);  // TODO read from real tables
        final ArrayList<HashMap<Long, Integer>>
                staticSimilarities = getStaticSimilarities(eigenUserIds,
                                                           alreadyKnows,
                                                           path_person_hasInterest_tag,
                                                           path_person_isLocatedIn_place,
                                                           path_person_studyAt_organisation,
                                                           path_person_workAt_organisation);  // TODO read from real tables
        final Double staticWeight = 0.3; // TODO get from config

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
            .aggregate(new CountActivitiesPerUser(), new GetUserSimilarities(eigenUserIds, alreadyKnows));
		// similaritiesPerPost.print().setParallelism(1);

		// Use another window to sum up the per-post similarities
		DataStream<ArrayList<ArrayList<Long>>> recommendations = similaritiesPerPost
            .windowAll(TumblingEventTimeWindows.of(Time.days(1))) // TODO (Time.hours(4), Time.hours(1)))
            .aggregate(new SimilarityAggregate(eigenUserIds), new GetTopFiveRecommendations(eigenUserIds, staticSimilarities, staticWeight));
		// recommendations.print().setParallelism(1);

		// execute program
		env.execute("Task 2 Friend Recommendation");
	}

    private static ArrayList<HashSet<Long>> getExistingFriendships(Long[] eigenUserIds, String csvPath) throws IOException {
        // use hashmap first for easy lookup
        HashMap<Long, HashSet<Long>> friendSets = new HashMap<>();
        for (Long userId : eigenUserIds) {
            friendSets.putIfAbsent(userId, new HashSet<>());
        }

        // store concerned relationships
        InputStream csvStream = new FileInputStream(csvPath);
        BufferedReader reader = new BufferedReader(new InputStreamReader(csvStream));
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


    private static void updateSimilarityWithOneCSV(ArrayList<HashMap<Long, Integer>> similarities,
                                                   Long[] eigenUserIds,
                                                   ArrayList<HashSet<Long>> alreadyKnows,
                                                   String csvPath) throws IOException {
        HashMap<Long, HashSet<Long>> setsPerUser = new HashMap<>();

        // read into a hashmap: userId -> set<objects>
        InputStream csvStream = new FileInputStream(csvPath);
        BufferedReader reader = new BufferedReader(new InputStreamReader(csvStream));
        reader.lines().skip(1).forEach(line -> {
            String[] splits = line.split("\\|");
            Long userId = Long.valueOf(splits[0]);
            Long objectId = Long.valueOf(splits[1]);
            setsPerUser.computeIfAbsent(userId, k -> new HashSet<>()).add(objectId);
        });
        reader.close();

        // update similarities with cardinality of intersection set
        for (int i = 0; i < eigenUserIds.length; i++) {
            HashSet<Long> eigenSet = setsPerUser.get(eigenUserIds[i]);
            if (eigenSet == null) continue;

            for (HashMap.Entry<Long, HashSet<Long>> elem : setsPerUser.entrySet()) {
                Long userId = elem.getKey();
                if (userId.equals(eigenUserIds[i]) || alreadyKnows.get(i).contains(userId)) continue; // remove self and already-friend

                HashSet<Long> userSet = elem.getValue();
                userSet.retainAll(eigenSet);
                similarities.get(i).merge(elem.getKey(), userSet.size(), Integer::sum);
            }
        }
    }


    private static ArrayList<HashMap<Long, Integer>> getStaticSimilarities(Long[] eigenUserIds,
                                                                           ArrayList<HashSet<Long>> alreadyKnows,
                                                                           String path_person_hasInterest_tag,
                                                                           String path_person_isLocatedIn_place,
                                                                           String path_person_studyAt_organisation,
                                                                           String path_person_workAt_organisation) throws IOException {
        // init
        ArrayList<HashMap<Long, Integer>> similarities = new ArrayList<>();
        for (int i = 0; i < eigenUserIds.length; i++) {
            similarities.add(new HashMap<>());
        }

        updateSimilarityWithOneCSV(similarities, eigenUserIds, alreadyKnows, path_person_hasInterest_tag);
        updateSimilarityWithOneCSV(similarities, eigenUserIds, alreadyKnows, path_person_isLocatedIn_place);
        updateSimilarityWithOneCSV(similarities, eigenUserIds, alreadyKnows, path_person_studyAt_organisation);
        updateSimilarityWithOneCSV(similarities, eigenUserIds, alreadyKnows, path_person_workAt_organisation);

        return similarities;
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
		private ArrayList<HashSet<Long>> alreadyKnows;

		GetUserSimilarities(Long[] eigenUserIds, ArrayList<HashSet<Long>> alreadyKnows) {
		    this.eigenUserIds = eigenUserIds;
		    this.alreadyKnows = alreadyKnows;
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
						if (!userId.equals(eigenUserIds[i]) && !alreadyKnows.get(i).contains(userId)) {
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
        ArrayList<HashMap<Long, Integer>> staticSimilarities;
        Double staticWeight, dynamicWeight;

		GetTopFiveRecommendations(Long[] eigenUserIds, ArrayList<HashMap<Long, Integer>> staticSimilarities, Double staticWeight) {
		    this.eigenUserIds = eigenUserIds;
		    this.staticSimilarities = staticSimilarities;
		    this.staticWeight = staticWeight;
		    this.dynamicWeight = 1.0 - staticWeight;
		}

        String prettify(TimeWindow w) {
            LocalDateTime start = LocalDateTime.ofInstant(Instant.ofEpochMilli(w.getStart()),
                    TimeZone.getTimeZone("GMT+0").toZoneId());
            LocalDateTime end = LocalDateTime.ofInstant(Instant.ofEpochMilli(w.getEnd()),
                    TimeZone.getTimeZone("GMT+0").toZoneId());
            return "[" + start + ", " + end + ")";
        }

        ArrayList<Tuple2<Double, Double>> getSimilarityRanges(ArrayList<HashMap<Long, Integer>> similarities) {
            ArrayList<Tuple2<Double, Double>> ret = new ArrayList<>();
            for (HashMap<Long, Integer> map : similarities) {
                Double lower = Double.POSITIVE_INFINITY, upper = Double.NEGATIVE_INFINITY;
                for (HashMap.Entry<Long, Integer> elem : map.entrySet()) {
                    Double val = elem.getValue().doubleValue();
                    if (val > upper) upper = val;
                    if (val < lower) lower = val;
                }
                ret.add(new Tuple2<>(lower, upper));
            }
            return ret;
        }

		public void process(Context context,
							Iterable<ArrayList<HashMap<Long, Integer>>> aggregations,
							Collector<ArrayList<ArrayList<Long>>> out) {

			ArrayList<HashMap<Long, Integer>> dynamicSimilarities = aggregations.iterator().next();
			ArrayList<Tuple2<Double, Double>> dynamicRanges = getSimilarityRanges(dynamicSimilarities);
            ArrayList<Tuple2<Double, Double>> staticRanges = getSimilarityRanges(staticSimilarities);
            System.out.println("Window: " + prettify(context.window()) + ", dynamicSimilarities: " + dynamicSimilarities);
            System.out.println("Window: " + prettify(context.window()) + ", staticSimilarities: " + staticSimilarities);

            ArrayList<ArrayList<Long>> recommendations = new ArrayList<>();

            for (int i = 0; i < eigenUserIds.length; i++) {
                // calculate final similarity and sort
                Tuple2<Double, Double> staticRange = staticRanges.get(i);
                Tuple2<Double, Double> dynamicRange = dynamicRanges.get(i);

                Double staticMin = staticRange.f0;
                Double staticSpan = staticRange.f1 - staticRange.f0;
                Double dynamicMin = dynamicRange.f0;
                Double dynamicSpan = dynamicRange.f1 - dynamicRange.f0;

                PriorityQueue<UserWithSimilarity> queue = new PriorityQueue<>(new UserWithSimilarityComparator());
                // for all users that have static similarity with eigen-user i
                for (HashMap.Entry<Long, Integer> elem : staticSimilarities.get(i).entrySet()) {
                    Long userId = elem.getKey();
                    Integer staticVal = elem.getValue();
                    Integer dynamicVal = dynamicSimilarities.get(i).getOrDefault(userId, 0);
                    dynamicSimilarities.get(i).remove(userId); // remove users that have both static and dynamic similarities with eigen-user i

                    Double staticPart = (staticSpan > 0.0) ? ((staticVal - staticMin) / staticSpan) : 1.0;
                    Double dynamicPart = (dynamicSpan > 0.0) ? ((dynamicVal - dynamicMin) / dynamicSpan) : 1.0;
                    queue.offer(new UserWithSimilarity(userId, staticPart * staticWeight + dynamicPart * dynamicWeight));
				}
				// the remaining users have dynamic similarity with eigen-user i, but no static similarity
                for (HashMap.Entry<Long, Integer> elem : dynamicSimilarities.get(i).entrySet()) {
                    Long userId = elem.getKey();
                    Integer dynamicVal = elem.getValue();
                    queue.offer(new UserWithSimilarity(userId, ((dynamicSpan > 0.0) ? ((dynamicVal - dynamicMin) / dynamicSpan) : 1.0) * dynamicWeight));
                }

                // get top 5
                ArrayList<Long> recommendationsPerUser = new ArrayList<>();
                while (!queue.isEmpty() && recommendationsPerUser.size() < 5) {
                    UserWithSimilarity pair = queue.poll();
				    recommendationsPerUser.add(pair.userId);
                    System.out.println("Window: " + prettify(context.window()) + ", recommend for " + eigenUserIds[i] + ": " + pair);
                }
                recommendations.add(recommendationsPerUser);
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

	public static class UserWithSimilarity {
	    Long userId;
	    Double similarity;

	    UserWithSimilarity(Long userId, Double similarity) { this.userId = userId; this.similarity = similarity; }

        @Override
        public String toString() {
            return "(userId: " + userId + ", similarity: " + similarity + ")";
        }
    }

    public static class UserWithSimilarityComparator implements Comparator<UserWithSimilarity>{
        // for descending order of similarity
        public int compare(UserWithSimilarity s1, UserWithSimilarity s2) {
            if (s1.similarity < s2.similarity)
                return 1;
            else if (s1.similarity > s2.similarity)
                return -1;
            return 0;
        }
    }

}


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

---------------------------------------
Test table: person_hasInterest_tag.csv
---------------------------------------
Person.id|Tag.id
10000|1
10000|2
10000|3
10000|4
10001|4
10001|3
10010|3
10011|4

---------------------------------------
Test table: person_isLocatedIn_place.csv
---------------------------------------
Person.id|Place.id
10000|1
10000|2
10000|3
10000|4
10001|4
10001|3
10010|3
10011|4

--------------------------------------------
Test table: person_studyAt_organisation.csv
--------------------------------------------
Person.id|Organisation.id|classYear
10000|1|2020
10000|2|2018
10000|3|2018
10000|4|2014
10001|4|2013
10001|3|2010
10010|3|2015
10011|4|1998

--------------------------------------------
Test table: person_workAt_organisation.csv
--------------------------------------------
Person.id|Organisation.id|workFrom
10000|1|2020
10000|2|2018
10000|3|2018
10000|4|2014
10001|4|2013
10001|3|2010
10010|3|2015
10011|4|1998
*/
