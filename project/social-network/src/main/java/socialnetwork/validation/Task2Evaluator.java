package socialnetwork.validation;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import scala.Int;
import socialnetwork.task.recommendation.FriendRecommender;
import socialnetwork.util.Activity;
import socialnetwork.util.Config;

import java.io.BufferedWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

import static socialnetwork.util.Helpers.getFileWriter;

public class Task2Evaluator extends SlidingWindowEvaluator {

    // input
    Integer[] eigenUserIds;

    // state
    List<Set<Integer>> alreadyKnows;
    List<Map<Integer, Integer>> staticSimilarities;
    Double staticWeight;
    Double dynamicWeight;

    // output
    private BufferedWriter recommendationWriter;

    public Task2Evaluator(String outputPath) {
        size = Time.hours(4).toMilliseconds();
        slide = Time.hours(1).toMilliseconds();

        eigenUserIds = Config.eigenUserIds;
        recommendationWriter = getFileWriter(outputPath);

        dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));

        alreadyKnows = FriendRecommender.getExistingFriendships(eigenUserIds);
        staticSimilarities = FriendRecommender.getStaticSimilarities(eigenUserIds, alreadyKnows);
        staticWeight = Config.staticWeight;
        dynamicWeight = 1.0 - staticWeight;
    }

    SimpleDateFormat dateFormat;


    Tuple2<Double, Double> getSimilarityRanges(Map<Integer, Integer> similarities) {
        Double lower = Double.POSITIVE_INFINITY, upper = Double.NEGATIVE_INFINITY;
        for (Map.Entry<Integer, Integer> elem : similarities.entrySet()) {
            Double val = elem.getValue().doubleValue();
            upper = Double.max(upper, val);
            lower = Double.min(lower, val);
        }
        return Tuple2.of(lower, upper);
    }

    void processWindow(List<Activity> window) {
        StringBuilder recommendationsOutput = new StringBuilder();

        // Count activities per post and per user
        Map<Integer, Map<Integer, Integer>> counts = new HashMap<>();  // user -> (post -> count)
        for (Activity activity : window) {
            Integer user = activity.getPersonId();
            Integer post = activity.getPostId();
            if (!counts.containsKey(user)) {
                counts.put(user, new HashMap<>());
            }
            counts.get(user).merge(post, 1, Integer::sum);
        }

        // Calculate similarities between every (eigen user, other user)
        for (int i = 0; i < eigenUserIds.length; i++) {
            Integer eigenUserId = eigenUserIds[i];
            Map<Integer, Integer> dynamicSimilarities = new HashMap<>();  // user id -> similarity
            Map<Integer, Integer> eigenCounts = counts.get(eigenUserId);

            // 1. calculate dynamic similarities
            // if this eigen user has activities in this window
            if (counts.containsKey(eigenUserId)) {
                // for every user
                for (Map.Entry<Integer, Map<Integer, Integer>> entry : counts.entrySet()) {
                    Integer userId = entry.getKey();
                    // eliminate the already friend users here, so that size of similarities can be reduced
                    if (!userId.equals(eigenUserId) && !alreadyKnows.get(i).contains(userId)) {
                        Map<Integer, Integer> userCounts = entry.getValue();
                        // for every post
                        for (Map.Entry<Integer, Integer> elem : eigenCounts.entrySet()) {
                            Integer postId = elem.getKey();
                            // if they both have activities on this post
                            if (userCounts.containsKey(postId)) {
                                Integer eigenCount = elem.getValue();
                                Integer userCount = userCounts.get(postId);
                                dynamicSimilarities.merge(userId, eigenCount * userCount, Integer::sum);
                            }
                        }
                    }
                }
            }

            // 2. add static similarities
            Tuple2<Double, Double> dynamicRange = getSimilarityRanges(dynamicSimilarities);
            Tuple2<Double, Double> staticRange = getSimilarityRanges(staticSimilarities.get(i));

            double staticMin = staticRange.f0;
            double staticSpan = staticRange.f1 - staticRange.f0;
            double dynamicMin = dynamicRange.f0;
            double dynamicSpan = dynamicRange.f1 - dynamicRange.f0;

            PriorityQueue<FriendRecommender.UserWithSimilarity> queue = new PriorityQueue<>(new FriendRecommender.UserWithSimilarityComparator());
            // for all users that have static similarity with eigen-user i
            for (Map.Entry<Integer, Integer> elem : staticSimilarities.get(i).entrySet()) {
                Integer userId = elem.getKey();
                Integer staticVal = elem.getValue();
                Integer dynamicVal = dynamicSimilarities.getOrDefault(userId, 0);
                dynamicSimilarities.remove(userId); // remove users that have both static and dynamic similarities with eigen-user i

                Double staticPart = (staticSpan > 0.0) ? ((staticVal - staticMin) / staticSpan) : 1.0;
                Double dynamicPart = (dynamicSpan > 0.0) ? ((dynamicVal - dynamicMin) / dynamicSpan) : 1.0;
                queue.offer(new FriendRecommender.UserWithSimilarity(userId, staticPart * staticWeight + dynamicPart * dynamicWeight));
            }
            // the remaining users have dynamic similarity with eigen-user i, but no static similarity
            for (Map.Entry<Integer, Integer> elem : dynamicSimilarities.entrySet()) {
                Integer userId = elem.getKey();
                Integer dynamicVal = elem.getValue();
                queue.offer(new FriendRecommender.UserWithSimilarity(userId, ((dynamicSpan > 0.0) ? ((dynamicVal - dynamicMin) / dynamicSpan) : 1.0) * dynamicWeight));
            }

            // get top 5
            List<Integer> recommendations = new ArrayList<>();
            while (!queue.isEmpty() && recommendations.size() < 5) {
                FriendRecommender.UserWithSimilarity pair = queue.poll();
                recommendations.add(pair.userId);
            }
            recommendationsOutput.append(Tuple3.of(currentEnd, eigenUserId, recommendations));
            recommendationsOutput.append("\n");
        }

        try {
            recommendationWriter.write(recommendationsOutput.toString());
            recommendationWriter.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();

        try {
            recommendationWriter.close();
        } catch (IOException e) {
            System.out.println("Failed to close file");
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        Task2Evaluator evaluator = new Task2Evaluator("expected-recommendations.txt");
        evaluator.run();
    }
}
