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

package socialnetwork.task.activepost;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import socialnetwork.task.TaskBase;
import socialnetwork.util.Activity;
import socialnetwork.util.Config;

import java.util.HashSet;
import java.util.Set;

public class ActivePostStatistician extends TaskBase <Activity> {
    private final static Logger logger = LoggerFactory.getLogger("Task1");
    private final OutputTag<Activity> lateTag = new OutputTag<Activity>("LATE:") {};

    @Override
    public void buildPipeline(StreamExecutionEnvironment env, DataStream<Activity> inputStream) {

        WindowedStream<Activity, Integer, TimeWindow> windowedStream = inputStream
                .keyBy(Activity::getPostId)
                .timeWindow(Time.minutes(30))
                .allowedLateness(Config.outOfOrdernessBound)
                .sideOutputLateData(lateTag);

        // 1. comments per active post, updated every 30 minutes
        SingleOutputStreamOperator<PostWithCount> commentsCountStream = windowedStream
                .aggregate(new CountMessages(Activity.ActivityType.Comment), new SetWindowEndAndKey())
                .keyBy(val -> val.postId)
                .timeWindow(Time.hours(12), Time.minutes(30))
                .aggregate(new SumUpCounts(), new SetWindowEndAndKey());

        commentsCountStream
                .getSideOutput(lateTag)
                .writeAsText(Config.lateCommentsOutputFilename, FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1)
                .name("late-comments");

        commentsCountStream
                .writeAsText(Config.commentCountsOutputFilename, FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1)
                .name("comment-counts");

        // 2. replies per active post, updated every 30 minutes
        SingleOutputStreamOperator<PostWithCount> repliesCountStream = windowedStream
                .aggregate(new CountMessages(Activity.ActivityType.Reply), new SetWindowEndAndKey())
                .keyBy(val -> val.postId)
                .timeWindow(Time.hours(12), Time.minutes(30))
                .aggregate(new SumUpCounts(), new SetWindowEndAndKey());

        repliesCountStream
                .writeAsText(Config.replyCountsOutputFilename, FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1)
                .name("reply-counts");

        repliesCountStream
                .getSideOutput(lateTag)
                .writeAsText(Config.lateRepliesOutputFilename, FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1)
                .name("late-replies");

        // 3. unique users per active post, updated every hour
        SingleOutputStreamOperator<PostWithCount> usersCountStream = windowedStream
                .aggregate(new CountUniqueUsers(), new AppendKey<>())
                .keyBy(val -> val.f0)
                .timeWindow(Time.hours(12), Time.hours(1))
                .aggregate(new UnionUniqueUsers(), new SetWindowEndAndKey());

        usersCountStream
                .writeAsText(Config.userCountsOutputFilename, FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1)
                .name("user-counts");
    }

    public class CountMessages implements AggregateFunction<Activity, Integer, PostWithCount> {
        private Activity.ActivityType type;

        private CountMessages(Activity.ActivityType type) {
            this.type = type;
        }

        @Override
        public Integer createAccumulator() {
            return 0;
        }

        @Override
        public Integer add(Activity value, Integer count) {
            return value.getType() == this.type ? count + 1 : count;
        }

        @Override
        public PostWithCount getResult(Integer aggregate) {
            PostWithCount.PostWithCountType type;
            switch (this.type) {
                case Reply:
                    type = PostWithCount.PostWithCountType.Reply;
                    break;
                case Comment:
                    type = PostWithCount.PostWithCountType.Comment;
                    break;
                default:
                    type = PostWithCount.PostWithCountType.Others;
                    logger.error("AggregateFunction CountMessages returning type Others with aggregate {}", aggregate);
            }
            return new PostWithCount(type, aggregate);
        }

        @Override
        public Integer merge(Integer acc1, Integer acc2) {
            return acc1 + acc2;
        }
    }

    public class SetWindowEndAndKey extends ProcessWindowFunction<PostWithCount, PostWithCount, Integer, TimeWindow> {
        @Override
        public void process(Integer key, Context context, Iterable<PostWithCount> counts, Collector<PostWithCount> collector) {
            PostWithCount count = counts.iterator().next();
            count.setWindowEnd(context.window().getEnd());
            count.setPostId(key);
            collector.collect(count);
        }
    }

    public static class PostWithCount {
        public enum PostWithCountType {
            Comment,
            Reply,
            Person,
            Others;

            static PostWithCountType fromString(String s) {
                if (s.equals("Comment")) return Comment;
                if (s.equals("Reply")) return Reply;
                if (s.equals("Person")) return Person;
                logger.error("Unknown PostWithCountType: {}", s);
                return Others;
            }

            static String toString(PostWithCountType t) {
                switch (t) {
                    case Comment: return "Comment";
                    case Reply: return "Reply";
                    case Person: return "Person";
                    default: return "Others";
                }
            }
        }

        public PostWithCountType type;
        public Integer postId;
        public Integer count;
        public Long windowEnd;

        PostWithCount(PostWithCountType t, Integer c) { type = t; count = c; }

        public void setWindowEnd(Long t) { this.windowEnd = t; }
        public void setPostId(Integer p) { this.postId = p; }

        public String toString() {
//            return "(" + windowEnd + ", post " + postId + ", " + PostWithCountType.toString(type) + ", " + count + ")";
            return "(" + windowEnd + "," + postId + "," + count + ")";
        }
    }


    public class SumUpCounts implements AggregateFunction<PostWithCount, PostWithCount, PostWithCount> {
        @Override
        public PostWithCount createAccumulator() {
            return new PostWithCount(PostWithCount.PostWithCountType.Others, 0);
        }

        @Override
        public PostWithCount add(PostWithCount val, PostWithCount acc) {
            acc.type = val.type;
            acc.postId = val.postId;
            acc.count += val.count;
            return acc;
        }

        @Override
        public PostWithCount getResult(PostWithCount acc) {
            return acc;
        }

        @Override
        public PostWithCount merge(PostWithCount acc1, PostWithCount acc2) {
            assert acc1.type.equals(acc2.type);
            assert acc1.postId.equals(acc2.postId);
            acc1.count += acc2.count;
            return acc1;
        }
    }

    public class CountUniqueUsers implements AggregateFunction<Activity, Set<Integer>, Set<Integer>> {
        @Override
        public Set<Integer> createAccumulator() {
            return new HashSet<>();
        }

        @Override
        public Set<Integer> add(Activity activity, Set<Integer> uniqueUsers) {
            Integer personId = activity.getPersonId();
            uniqueUsers.add(personId);
            return uniqueUsers;
        }

        @Override
        public Set<Integer> getResult(Set<Integer> uniqueUsers) {
            return uniqueUsers;
        }

        @Override
        public Set<Integer> merge(Set<Integer> s1, Set<Integer> s2) {
            s1.addAll(s2);
            return s1;
        }
    }

    public class UnionUniqueUsers implements AggregateFunction<Tuple2<Integer, Set<Integer>>, Set<Integer>, PostWithCount> {
        @Override
        public Set<Integer> createAccumulator() {
            return new HashSet<>();
        }

        @Override
        public Set<Integer> add(Tuple2<Integer, Set<Integer>> val, Set<Integer> acc) {
            acc.addAll(val.f1);
            return acc;
        }

        @Override
        public PostWithCount getResult(Set<Integer> acc) {
            return new PostWithCount(PostWithCount.PostWithCountType.Person, acc.size());
        }

        @Override
        public Set<Integer> merge(Set<Integer> acc1, Set<Integer> acc2) {
            acc1.addAll(acc2);
            return acc1;
        }
    }

    public class AppendKey<V> extends ProcessWindowFunction<V, Tuple2<Integer, V>, Integer, TimeWindow> {
        @Override
        public void process(Integer key, Context context, Iterable<V> values, Collector<Tuple2<Integer, V>> collector) {
            collector.collect(Tuple2.of(key, values.iterator().next()));
        }
    }
}