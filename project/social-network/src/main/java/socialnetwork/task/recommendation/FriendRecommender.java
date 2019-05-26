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

package socialnetwork.task.recommendation;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import socialnetwork.task.TaskBase;
import socialnetwork.util.Activity;
import socialnetwork.util.Config;

import java.io.*;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.*;

public class FriendRecommender extends TaskBase<Activity> {
    
    private final static Logger logger = LoggerFactory.getLogger("Task2");
    private final Integer[] eigenUserIds = Config.eigenUserIds;
    private final OutputTag<Activity> lateTag = new OutputTag<Activity>("Task2Late") {};

    public void buildPipeline(StreamExecutionEnvironment env, DataStream<Activity> inputStream) {
        final List<Set<Integer>> alreadyKnows = getExistingFriendships(eigenUserIds);
        logger.info("Loaded existing friendships");

        final List<Map<Integer, Integer>> staticSimilarities = getStaticSimilarities(eigenUserIds, alreadyKnows);
        logger.info("Pre-computed static similarities");

        // get per-post similarities with a keyed sliding window
        WindowedStream<Activity, Integer, TimeWindow> windowedStream = inputStream
            .keyBy(Activity::getPostId)
            .window(SlidingEventTimeWindows.of(Time.hours(4), Time.hours(1)))
            .allowedLateness(Config.outOfOrdernessBound)
            .sideOutputLateData(lateTag);

        SingleOutputStreamOperator<Tuple2<Integer, Map<Integer, Integer>>> similaritiesPerPost = windowedStream
            .aggregate(new CountActivitiesPerUser(), new GetUserSimilarities(alreadyKnows));
        // similaritiesPerPost.print().setParallelism(1);

        similaritiesPerPost
                .getSideOutput(lateTag)
                .writeAsText("log/task2-late.txt", FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1)
                .name("task2-late");

        // Use another window to sum up the per-post similarities
        DataStream<Tuple3<Long, Integer, List<Integer>>> recommendations = similaritiesPerPost
                .keyBy(tuple -> tuple.f0)
                .timeWindow(Time.hours(1))
                .aggregate(new SimilarityAggregate(), new GetTopFiveRecommendations(staticSimilarities, Config.staticWeight));

//        recommendations.print().setParallelism(1);
        recommendations
                .writeAsText("log/recommendations.txt", FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1)
                .name("recommendations");
    }

    public void buildTestPipeline(StreamExecutionEnvironment env) {
        final List<Set<Integer>>
                alreadyKnows = getExistingFriendships(eigenUserIds);

        final List<Map<Integer, Integer>>
                staticSimilarities = getStaticSimilarities(eigenUserIds, alreadyKnows);

        DataStream<Activity> input =
            env.readTextFile(System.getProperty("user.home") + "/repo/eth-dspa-2019/project/data/task2.txt")
               .map(Activity::fromString)
               .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Activity>(Config.outOfOrdernessBound) {
                   public long extractTimestamp(Activity a) {
                       return a.getCreationTimestamp();
                   }
               });
        input.print().setParallelism(1);

        // get per-post similarities with a keyed sliding window
        DataStream<Tuple2<Integer, Map<Integer, Integer>>> similaritiesPerPost = input
                .keyBy(Activity::getPostId)
                .window(SlidingEventTimeWindows.of(Time.hours(4), Time.hours(1)))
                .aggregate(new CountActivitiesPerUser(), new GetUserSimilarities(alreadyKnows));
        similaritiesPerPost.print().setParallelism(1);

        // Use another window to sum up the per-post similarities
        DataStream<Tuple3<Long, Integer, List<Integer>>> recommendations = similaritiesPerPost
                .keyBy(tuple -> tuple.f0)
                .timeWindow(Time.hours(1))
                .aggregate(new SimilarityAggregate(), new GetTopFiveRecommendations(staticSimilarities, Config.staticWeight));

        recommendations.print().setParallelism(1);
    }

    public static List<Set<Integer>> getExistingFriendships(Integer[] eigenUserIds) {
        // use hashmap first for easy lookup
        Map<Integer, Set<Integer>> friendSets = new HashMap<>();
        for (Integer userId : eigenUserIds) {
            friendSets.putIfAbsent(userId, new HashSet<>());
        }

        // store concerned relationships
        try {
            InputStream csvStream = new FileInputStream(Config.person_knows_person_1K);
            BufferedReader reader = new BufferedReader(new InputStreamReader(csvStream));
            // skip the header of the csv
            reader.lines().skip(1).forEach(line -> {
                String[] splits = line.split("\\|");
                Integer knower = Integer.valueOf(splits[0]);
                Integer knowee = Integer.valueOf(splits[1]);
                if (friendSets.containsKey(knower)) {
                    friendSets.get(knower).add(knowee);
                }
            });
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }

        // put into arraylist
        List<Set<Integer>> retList = new ArrayList<>();
        for (Integer userId : eigenUserIds) {
            retList.add(friendSets.get(userId));
        }
        return retList;
    }


    public static void updateSimilarityWithOneCSV(Integer[] eigenUserIds,
                                                  List<Map<Integer, Integer>> similarities,
                                                  List<Set<Integer>> alreadyKnows,
                                                  String csvPath) {
        Map<Integer, Set<Integer>> setsPerUser = new HashMap<>();

        // read into a hashmap: userId -> set<objects>
        try {
            InputStream csvStream = new FileInputStream(csvPath);
            BufferedReader reader = new BufferedReader(new InputStreamReader(csvStream));
            reader.lines().skip(1).forEach(line -> {
                String[] splits = line.split("\\|");
                Integer userId = Integer.valueOf(splits[0]);
                Integer objectId = Integer.valueOf(splits[1]);
                setsPerUser.computeIfAbsent(userId, k -> new HashSet<>()).add(objectId);
            });
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }

        // update similarities with cardinality of intersection set
        for (int i = 0; i < eigenUserIds.length; i++) {
            Set<Integer> eigenSet = setsPerUser.get(eigenUserIds[i]);
            if (eigenSet == null) continue;

            for (Map.Entry<Integer, Set<Integer>> elem : setsPerUser.entrySet()) {
                Integer userId = elem.getKey();
                if (userId.equals(eigenUserIds[i]) || alreadyKnows.get(i).contains(userId)) continue; // remove self and already-friend

                Set<Integer> userSet = elem.getValue();
                userSet.retainAll(eigenSet);
                similarities.get(i).merge(elem.getKey(), userSet.size(), Integer::sum);
            }
        }
    }


    public static List<Map<Integer, Integer>> getStaticSimilarities(Integer[] eigenUserIds, List<Set<Integer>> alreadyKnows) {
        // init
        List<Map<Integer, Integer>> similarities = new ArrayList<>();
        for (int i = 0; i < eigenUserIds.length; i++) {
            similarities.add(new HashMap<>());
        }

        updateSimilarityWithOneCSV(eigenUserIds, similarities, alreadyKnows, Config.person_hasInterest_tag_1K);
        updateSimilarityWithOneCSV(eigenUserIds, similarities, alreadyKnows, Config.person_isLocatedIn_place_1K);
        updateSimilarityWithOneCSV(eigenUserIds, similarities, alreadyKnows, Config.person_studyAt_organisation_1K);
        updateSimilarityWithOneCSV(eigenUserIds, similarities, alreadyKnows, Config.person_workAt_organisation_1K);

        return similarities;
    }

    private static class CountActivitiesPerUser
            implements AggregateFunction<Activity, Map<Integer, Integer>, Map<Integer, Integer>> {

        @Override
        public Map<Integer, Integer> createAccumulator() {
            return new HashMap<>();  // userId -> count
        }

        @Override
        public Map<Integer, Integer> add(Activity value, Map<Integer, Integer> accumulator) {
            accumulator.merge(value.getPersonId(), 1, Integer::sum);
            return accumulator;
        }

        @Override
        public Map<Integer, Integer> getResult(Map<Integer, Integer> accumulator) {
            return accumulator;
        }

        @Override
        public Map<Integer, Integer> merge(Map<Integer, Integer> r1, Map<Integer, Integer> r2) {
            for (Map.Entry<Integer, Integer> elem : r1.entrySet()) {
                r2.merge(elem.getKey(), elem.getValue(), Integer::sum);
            }
            return r2;
        }
    }

    public static class GetUserSimilarities
            extends ProcessWindowFunction<Map<Integer, Integer>, Tuple2<Integer, Map<Integer, Integer>>, Integer, TimeWindow> {

        private Integer[] eigenUserIds;
        private List<Set<Integer>> alreadyKnows;

        GetUserSimilarities(List<Set<Integer>> alreadyKnows) {
            this.eigenUserIds = Config.eigenUserIds;
            this.alreadyKnows = alreadyKnows;
        }

        String prettify(TimeWindow w) {
            LocalDateTime start = LocalDateTime.ofInstant(Instant.ofEpochMilli(w.getStart()),
                    TimeZone.getTimeZone("GMT+0").toZoneId());
            LocalDateTime end = LocalDateTime.ofInstant(Instant.ofEpochMilli(w.getEnd()),
                    TimeZone.getTimeZone("GMT+0").toZoneId());
            return "{" + start + "-" + end + "}";
        }

        @Override
        public void process(Integer postId, Context context, Iterable<Map<Integer, Integer>> input, Collector<Tuple2<Integer, Map<Integer, Integer>>> out) {
            // init similarity matrix
            List<HashMap<Integer, Integer>> similarities = new ArrayList<>();  // similarities[eigenUsers][allUsers] -> similarity
            for (int i = 0; i < eigenUserIds.length; ++i) {
                similarities.add(new HashMap<>());
            }

            // count activities for every user
            Map<Integer, Integer> counts = input.iterator().next();  // userId -> count

            // calculate similarity
            for (int i = 0; i < eigenUserIds.length; ++i) {
                Integer eigenCount = counts.get(eigenUserIds[i]);
                if (eigenCount != null) {
                    for (Map.Entry<Integer, Integer> elem : counts.entrySet()) {
                        Integer userId = elem.getKey();
                        if (!userId.equals(eigenUserIds[i]) && !alreadyKnows.get(i).contains(userId)) {
                            // eliminate the already friend users here, so that size of similarities can be reduced (less communication overhead)
                            Integer userCount = elem.getValue();
                            similarities.get(i).put(userId, eigenCount * userCount);
                        }
                    }
                }
            }
//            logger.debug("PostId: " + postId + ", Window: " + prettify(context.window()) + ", similarities: " + similarities);
            int eigenUserIndex = 0;
            for(Map<Integer, Integer> eigenUserMap : similarities) {
                out.collect(Tuple2.of(eigenUserIndex++, eigenUserMap));
            }
        }
    }

    private static class SimilarityAggregate
            implements AggregateFunction<Tuple2<Integer, Map<Integer, Integer>>, Tuple2<Integer, Map<Integer, Integer>>, Tuple2<Integer, Map<Integer, Integer>>> {

        @Override
        public Tuple2<Integer, Map<Integer, Integer>> createAccumulator() {
            return Tuple2.of(-1, new HashMap<>());
        }

        @Override
        public Tuple2<Integer, Map<Integer, Integer>> add(Tuple2<Integer, Map<Integer, Integer>> value, Tuple2<Integer, Map<Integer, Integer>> accumulator) {
            accumulator.f0 = value.f0;
            for (Map.Entry<Integer, Integer> elem : value.f1.entrySet()) {
                accumulator.f1.merge(elem.getKey(), elem.getValue(), Integer::sum);
            }
            return accumulator;
        }

        @Override
        public Tuple2<Integer, Map<Integer, Integer>> getResult(Tuple2<Integer, Map<Integer, Integer>> accumulator) {
            return accumulator;
        }

        @Override
        public Tuple2<Integer, Map<Integer, Integer>> merge(Tuple2<Integer, Map<Integer, Integer>> r1, Tuple2<Integer, Map<Integer, Integer>> r2) {
            r2.f0 = r1.f0;
            for (Map.Entry<Integer, Integer> elem : r1.f1.entrySet()) {
                r2.f1.merge(elem.getKey(), elem.getValue(), Integer::sum);
            }
            return r2;
        }
    }

    private static class GetTopFiveRecommendations
            extends ProcessWindowFunction<Tuple2<Integer, Map<Integer, Integer>>, Tuple3<Long, Integer, List<Integer>>, Integer, TimeWindow> {

        private Integer[] eigenUserIds;
        List<Map<Integer, Integer>> staticSimilarities;
        Double staticWeight, dynamicWeight;

        GetTopFiveRecommendations(List<Map<Integer, Integer>> staticSimilarities, Double staticWeight) {
            this.eigenUserIds = Config.eigenUserIds;
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

        Tuple2<Double, Double> getSimilarityRanges(Map<Integer, Integer> similarities) {
            Double lower = Double.POSITIVE_INFINITY, upper = Double.NEGATIVE_INFINITY;
            for (Map.Entry<Integer, Integer> elem : similarities.entrySet()) {
                Double val = elem.getValue().doubleValue();
                if (val > upper) upper = val;
                if (val < lower) lower = val;
            }
            return Tuple2.of(lower, upper);
        }

        @Override
        public void process(Integer key,
                            Context context,
                            Iterable<Tuple2<Integer, Map<Integer, Integer>>> aggregations,
                            Collector<Tuple3<Long, Integer, List<Integer>>> out) {
            Tuple2<Integer, Map<Integer, Integer>> input = aggregations.iterator().next();
            int eigenUserIndex = input.f0;
            int eigenUserId = eigenUserIds[eigenUserIndex];
            Map<Integer, Integer> dynamicSimilarities = input.f1;
            Tuple2<Double, Double> dynamicRange = getSimilarityRanges(dynamicSimilarities);
            Tuple2<Double, Double> staticRange = getSimilarityRanges(staticSimilarities.get(eigenUserIndex));
//            logger.debug("Window: " + prettify(context.window()) + ", dynamicSimilarities: " + dynamicSimilarities);
//            logger.debug("Window: " + prettify(context.window()) + ", staticSimilarities: " + staticSimilarities);

            double staticMin = staticRange.f0;
            double staticSpan = staticRange.f1 - staticRange.f0;
            double dynamicMin = dynamicRange.f0;
            double dynamicSpan = dynamicRange.f1 - dynamicRange.f0;

            PriorityQueue<UserWithSimilarity> queue = new PriorityQueue<>(new UserWithSimilarityComparator());
            // for all users that have static similarity with eigen-user i
            for (Map.Entry<Integer, Integer> elem : staticSimilarities.get(eigenUserIndex).entrySet()) {
                Integer userId = elem.getKey();
                Integer staticVal = elem.getValue();
                Integer dynamicVal = dynamicSimilarities.getOrDefault(userId, 0);
                dynamicSimilarities.remove(userId); // remove users that have both static and dynamic similarities with eigen-user i

                Double staticPart = (staticSpan > 0.0) ? ((staticVal - staticMin) / staticSpan) : 1.0;
                Double dynamicPart = (dynamicSpan > 0.0) ? ((dynamicVal - dynamicMin) / dynamicSpan) : 1.0;
                queue.offer(new UserWithSimilarity(userId, staticPart * staticWeight + dynamicPart * dynamicWeight));
            }
            // the remaining users have dynamic similarity with eigen-user i, but no static similarity
            for (Map.Entry<Integer, Integer> elem : dynamicSimilarities.entrySet()) {
                Integer userId = elem.getKey();
                Integer dynamicVal = elem.getValue();
                queue.offer(new UserWithSimilarity(userId, ((dynamicSpan > 0.0) ? ((dynamicVal - dynamicMin) / dynamicSpan) : 1.0) * dynamicWeight));
            }

            // get top 5
            List<Integer> recommendations = new ArrayList<>();
            while (!queue.isEmpty() && recommendations.size() < 5) {
                UserWithSimilarity pair = queue.poll();
                recommendations.add(pair.userId);
//                    logger.debug("Window: " + prettify(context.window()) + ", recommend for " + eigenUserIds[i] + ": " + pair);
            }
            out.collect(Tuple3.of(context.window().getEnd(), eigenUserId, recommendations));
        }
    }

    public static class UserWithSimilarity {
        public Integer userId;
        public Double similarity;

        public UserWithSimilarity(Integer userId, Double similarity) { this.userId = userId; this.similarity = similarity; }

        @Override
        public String toString() {
            return "(userId: " + userId + ", similarity: " + similarity + ")";
        }
    }

    public static class UserWithSimilarityComparator implements Comparator<UserWithSimilarity> {
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
