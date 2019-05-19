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
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import socialnetwork.task.TaskBase;
import socialnetwork.util.Activity;

import java.io.*;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class ActivePostStatistician extends TaskBase <Activity, ActivePostStatistician.PostWithCount> {
    final static Logger logger = LoggerFactory.getLogger("Task1");

    @Override
    public DataStream<PostWithCount> buildPipeline(StreamExecutionEnvironment env, DataStream<Activity> inputStream) {

//        WindowedStream<Tuple3<Long, Data_Type, Message>, Long, TimeWindow> windowedStream = stream
//                .keyBy(val -> val.f0)
//                .timeWindow(Time.hours(12), Time.minutes(30))
//                .allowedLateness(Config.MAX_DELAY)
//                .trigger(ExactlyOnceEventTimeTrigger.create())
//                .sideOutputLateData(lateTag);
//
//        SingleOutputStreamOperator<Tuple3<Long, Long, Long>> commentsCountStream = windowedStream
//                .aggregate(CountMessages.ofType(Data_Type.COMMENT), new AppendWindowStartAndKey());
//
//        commentsCountStream
//                .getSideOutput(lateTag)
//                .writeAsText("late-messages.txt", FileSystem.WriteMode.OVERWRITE)
//                .setParallelism(1);
//
//        commentsCountStream
//                .writeAsText("comment-counts.txt", FileSystem.WriteMode.OVERWRITE)
//                .setParallelism(1);
//
//        SingleOutputStreamOperator<Tuple3<Long, Long, Long>> repliesCountStream = windowedStream
//                .aggregate(CountMessages.ofType(Data_Type.REPLY), new AppendWindowStartAndKey());
//
//        repliesCountStream
//                .writeAsText("reply-counts.txt", FileSystem.WriteMode.OVERWRITE)
//                .setParallelism(1);
//
//        repliesCountStream
//                .getSideOutput(lateTag)
//                .writeAsText("late-messages.txt", FileSystem.WriteMode.OVERWRITE)
//                .setParallelism(1);

        return null;
    }

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
    }

    public class PostWithCount {
        public PostWithCountType type;
        public Integer count;

        PostWithCount(PostWithCountType t, Integer c) { type = t; count = c; }
    }
}