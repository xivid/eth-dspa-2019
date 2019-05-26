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

package socialnetwork.task.anomalydetection;

import org.apache.commons.math3.analysis.function.Signum;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
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

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static socialnetwork.util.Config.HOUR;

public class AnomalousUserDetector extends TaskBase <Activity> {
    private final static Logger logger = LoggerFactory.getLogger("Task3");
    private final OutputTag<Activity> lateTag = new OutputTag<Activity>("LATE:") {};

    @Override
    public void buildPipeline(StreamExecutionEnvironment env, DataStream<Activity> inputStream) {

        DataStream<Tuple3<Long, Integer, Features>> userFeatureStream = inputStream
                .keyBy(Activity::getPersonId)
                .flatMap(new ComputeFeatures());

        DataStream<Tuple2<Long, Integer>> flaggedUserStream = userFeatureStream
                .broadcast()
                .flatMap(new UnusualUserDetector());

        flaggedUserStream
                .writeAsText(Config.anomaliesOutputFilename, FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1)
                .name("anomalies");
    }


    public static class Features {
        // 3-moving-average of activity frequency (activities/hour), -1 if haven't seen as many as 3 activities yet
        public Double frequency;
        // length of last comment/post, -1 if last activity is not comment/post
        public Integer lastCommentLen;
        public Integer lastPostLen;
        // Unique words ratio of last comment/post, -1 if last activity is not comment/post
        public Double lastCommentRatio;
        public Double lastPostRatio;

        public Features() {
            frequency = -1.0;
            lastCommentLen = -1;
            lastPostLen = -1;
            lastCommentRatio = -1.0;
            lastPostRatio = -1.0;
        }

        public Features(Double _frequency, Integer _lastCommentLen, Integer _lastPostLen, Double _lastCommentRatio, Double _lastPostRatio) {
            frequency = _frequency;
            lastCommentLen = _lastCommentLen;
            lastPostLen = _lastPostLen;
            lastCommentRatio = _lastCommentRatio;
            lastPostRatio = _lastPostRatio;
        }

        public Features(Features f) {
            frequency = f.frequency;
            lastCommentLen = f.lastCommentLen;
            lastPostLen = f.lastPostLen;
            lastCommentRatio = f.lastCommentRatio;
            lastPostRatio = f.lastPostRatio;
        }

        public void update(Activity activity, Tuple3<Long, Long, Long> lastThreeActivities) {
            // update frequency with lastThreeActivities
            if (lastThreeActivities.f2 != -1) {
                frequency = 3.0 * HOUR / Double.max(1, lastThreeActivities.f0 - lastThreeActivities.f2);
            } else {
                frequency = -1.0;
            }

            // update lengths and ratios
            if (activity instanceof Activity.Comment) // strictly the same as (activity.getType() == Activity.ActivityType.Comment || activity.getType() == Activity.ActivityType.Reply)
            {
                Activity.Comment c = (Activity.Comment) activity;
                String s = c.getContent();
                lastCommentLen = s.length();
                lastCommentRatio = uniqueWordsRatio(s);
                lastPostRatio = -1.0;
                lastPostLen = -1;
            }
            else if (activity instanceof Activity.Post)
            {
                Activity.Post p = (Activity.Post) activity;
                String s = p.getContent();
                lastPostLen = s.length();
                lastPostRatio = uniqueWordsRatio(s);
                lastCommentRatio = -1.0;
                lastCommentLen = -1;
            } else {
                lastCommentLen = -1;
                lastPostLen = -1;
                lastCommentRatio = -1.0;
                lastPostRatio = -1.0;
            }
        }
    }

    public static class Signatures {
        // 3-moving-average of activity frequency (activities/hour), -1 if haven't seen as many as 3 activities yet
        public Double avgFrequency = 0.0;
        public Integer avgFrequencyCount = 0;
        // length of last comment/post, -1 if last activity is not comment/post
        public Integer avgCommentLen = 0;
        public Integer avgCommentLenCount = 0;
        public Integer avgPostLen = 0;
        public Integer avgPostLenCount = 0;
        // Unique words ratio of last comment/post, -1 if last activity is not comment/post
        public Double avgCommentRatio = 0.0;
        public Integer avgCommentRatioCount = 0;
        public Double avgPostRatio = 0.0;
        public Integer avgPostRatioCount = 0;

        public void update(Features f) {
            if (f.frequency != -1.0) {
                avgFrequency = (avgFrequency * avgFrequencyCount + f.frequency) / (avgFrequencyCount + 1);
                avgFrequencyCount++;
            }

            if (f.lastCommentLen != -1) {
                avgCommentLen = (avgCommentLen * avgCommentLenCount + f.lastCommentLen) / (avgCommentLenCount + 1);
                ++avgCommentLenCount;
            }

            if (f.lastPostLen != -1) {
                avgPostLen = (avgPostLen * avgPostLenCount + f.lastPostLen) / (avgPostLenCount + 1);
                ++avgPostLenCount;
            }

            if (f.lastCommentRatio != -1.0) {
                avgCommentRatio = (avgCommentRatio * avgCommentRatioCount + f.lastCommentRatio) / (avgCommentRatioCount + 1);
                ++avgCommentRatioCount;
            }

            if (f.lastPostRatio != -1.0) {
                avgPostRatio = (avgPostRatio * avgPostRatioCount + f.lastPostRatio) / (avgPostRatioCount + 1);
                ++avgPostRatioCount;
            }
        }

        // TODO: change the threshold to avg+-stdev, where stdev is the rolling std of last 100 seen values?
        public boolean isNormal(Features f) {
            if (f.frequency != -1.0 && f.frequency > avgFrequency * 2)
                return false;
            if (f.lastCommentLen != -1 && (f.lastCommentLen < avgCommentLen * 0.5 || f.lastCommentLen > avgCommentLen * 2))
                return false;
            if (f.lastPostLen != -1 && (f.lastPostLen < avgPostLen * 0.5 || f.lastPostLen > avgPostLen * 2))
                return false;
            if (f.lastCommentRatio != -1 && (f.lastCommentRatio < avgCommentRatio * 0.5 || f.lastCommentRatio > avgCommentRatio * 2))
                return false;
            if (f.lastPostRatio != -1 && (f.lastPostRatio < avgPostRatio * 0.5 || f.lastPostRatio > avgPostRatio * 2))
                return false;
            return true;
        }
    }

    public static Double uniqueWordsRatio(String s) {
        String[] splits = s.split("\\s+");
        Set<String> uniqueWords = new HashSet<>(Arrays.asList(splits));
        return 1.0 * uniqueWords.size() / splits.length;
    }

    public static class ComputeFeatures extends RichFlatMapFunction<
            Activity, Tuple3<Long, Integer, Features>> {

        private transient ValueState<Features> userFeatures;
        private transient ValueState<Tuple3<Long, Long, Long>> userLastThreeActivities;

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Features> userFeaturesDescriptor =
                    new ValueStateDescriptor<Features>(
                            "userFeatures",
                            TypeInformation.of(new TypeHint<Features>() {}),
                            new Features()
                    );
            ValueStateDescriptor<Tuple3<Long, Long, Long>> userLastThreeActivitiesDescriptor =
                    new ValueStateDescriptor<Tuple3<Long, Long, Long>>(
                            "userLastThreeActivities",
                            TypeInformation.of(new TypeHint<Tuple3<Long, Long, Long>>() {}),
                            Tuple3.of(-1L, -1L, -1L)
                    );
            userFeatures = getRuntimeContext().getState(userFeaturesDescriptor);
            userLastThreeActivities = getRuntimeContext().getState(userLastThreeActivitiesDescriptor);
        }

        @Override
        public void flatMap(Activity in,
                            Collector<Tuple3<Long, Integer, Features>> out) throws IOException {
            Features currentUserFeatures = userFeatures.value();
            Tuple3<Long, Long, Long> currentUserLastThreeActivities = userLastThreeActivities.value();

            currentUserLastThreeActivities.f2 = currentUserLastThreeActivities.f1;
            currentUserLastThreeActivities.f1 = currentUserLastThreeActivities.f0;
            currentUserLastThreeActivities.f0 = in.getCreationTimestamp();
            currentUserFeatures.update(in, currentUserLastThreeActivities);

            out.collect(new Tuple3<>(in.getCreationTimestamp(), in.getPersonId(), new Features(currentUserFeatures)));
        }
    }

    public static class UnusualUserDetector extends RichFlatMapFunction<
            Tuple3<Long, Integer, Features>, Tuple2<Long, Integer>> {

        private Signatures globalSignatures;
        private int parallelInstanceId;
        private int numParallelInstances;

        @Override
        public void open(Configuration parameters) {
            globalSignatures = new Signatures();
            parallelInstanceId = getRuntimeContext().getIndexOfThisSubtask();
            numParallelInstances = getRuntimeContext().getNumberOfParallelSubtasks();
            logger.info("UnusualUserDetector init {}/{}", parallelInstanceId, numParallelInstances);
        }

        @Override
        public void flatMap(Tuple3<Long, Integer, Features> in,
                            Collector<Tuple2<Long, Integer>> out) throws IOException {
            // update global signatures (each subtask keeps its own copy, but it's updated by all activities)
            globalSignatures.update(in.f2);

            // if I am responsible for this activity
            if (in.f1 % numParallelInstances == parallelInstanceId && !globalSignatures.isNormal(in.f2)) {
                out.collect(new Tuple2<>(in.f0, in.f1));
            }
        }
    }
}
