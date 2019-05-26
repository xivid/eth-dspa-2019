package socialnetwork.validation;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import socialnetwork.Producer;
import socialnetwork.util.Activity;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.*;

import static socialnetwork.util.Helpers.getFileWriter;
import static socialnetwork.task.anomalydetection.AnomalousUserDetector.*;

/**
 * From the resolved stream, we key by user id, and keep track of the following 5 features for each user:
 * - activity frequency (moving average of 3 last activities), which is defined as 3/max(latestActivity.timestamp - 3thLatestActivity.timestamp, 1ms)*HOUR, meaning how many activities this user will generate per hour if he keeps the same frequency of his last 3 activities;
 * - last comment length, equals to -1 if the latest activity is not a comment;
 * - last post length, equals to -1 if the latest activity is not a post;
 * - unique word ratio of last comment, equals to -1 if the latest activity is not a comment;
 * - unique word ratio of last post, equals to -1 if the latest activity is not a post.
 * The output of this operator is Tuple2(userId, features).
 *
 * Then the Tuple2 is fed into "SignatureUpdater", which is a process function that:
 * - maintains a state `signature`, which is the whole-history-average value of each feature (update the corresponding component only if the incoming value is not -1);
 * - outputs Tuple3(signature, userId, features). (userId and features are simply copied from the input Tuple2)
 *
 * Finally, the output of SignatureUpdater is keyed by userId, and passed to "UnusualUserDetector" that:
 * - compare `features` and `signature`, if there exists one component of `features` that is smaller than (for activity frequency, it's larger than) the corresponding component of `signature`, output the userId, otherwise no output.
 *
 * Why these 5 features are chosen:
 * - activity frequency: from the dataset we can easily find a very obvious spam pattern, which is a sequence of posts/comments created within a few seconds by the same user, e.g. lines 3~7 in post_event_stream.csv;
 * - last comment/post length: most comments/posts have very similar length, but those spams are often very short (actually, most of their contents are empty as I observe);
 * - unique word ratio: suggested by the task description, we'd better do this. Comments/Posts are separated because their "normal thresholds" are very likely to be different. The task also asked us to somehow differ the "normal ratio".
 */

public class Task3Evaluator {
    private BufferedWriter writer;

    public Task3Evaluator(String outputPath) {
        writer = getFileWriter(outputPath);
    }


    public void run() throws Exception {
        StringBuilder output = new StringBuilder();

        // get stream of activities and resolve the mappings
        TreeMap<Long, List<Activity>> data = Producer.readCleanedStreams(true, 0);
        resolveMappings(data);

        Signatures globalSignatures = new Signatures();
        Map<Integer, Features> featuresPerUser = new HashMap<>();
        Map<Integer, Tuple3<Long, Long, Long>> lastThreeActivitiesPerUser = new HashMap<>();

        while (!data.isEmpty()) {
            Map.Entry<Long, List<Activity>> entry = data.pollFirstEntry();
            Long timestamp = entry.getKey();
            List<Activity> activities = entry.getValue();

            for (Activity activity : activities) {
                Integer userId = activity.getPersonId();

                // create records for new user
                if (!featuresPerUser.containsKey(userId)) {
                    featuresPerUser.put(userId, new Features());
                }
                if (!lastThreeActivitiesPerUser.containsKey(userId)) {
                    lastThreeActivitiesPerUser.put(userId, new Tuple3<>(-1L, -1L, -1L));
                }

                // shift right, move new timestamp in
                lastThreeActivitiesPerUser.compute(userId, (k, t) -> new Tuple3<>(activity.getCreationTimestamp(), t.f0, t.f1));

                // update features of this user
                Features features = featuresPerUser.get(userId);
                features.update(activity, lastThreeActivitiesPerUser.get(userId));

                // use these features to update the signature
                globalSignatures.update(features);
                
                // detect whether these features are normal or not, if not, output the userId
                if (!globalSignatures.isNormal(features)) {
                    output.append(Tuple2.of(timestamp, userId));
                    output.append("\n");
                }
            }
        }


        try {
            writer.write(output.toString());
            writer.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("Done.");
    }

    private void resolveMappings(final SortedMap<Long,  List<Activity>> data) {
        final Map<Integer, Integer> childToParentMappings;
        childToParentMappings = new HashMap<>();

        for(List<Activity> list : data.values()) {
            for(Activity activity : list) {
                if(activity instanceof Activity.Tombstone) {
                    continue;
                }

                // Skip posts, because post ids overlap with comment ids
                if(activity.getType() == Activity.ActivityType.Post) {
                    continue;
                }

                // Skip likes, because post ids overlap with comment ids
                if(activity.getType() == Activity.ActivityType.Like) {
                    continue;
                }

                Integer parent = activity.getPostId();

                // update postid field for replies
                if(activity.getType() == Activity.ActivityType.Reply) {
                    Activity.Reply r = (Activity.Reply) activity;
                    parent = childToParentMappings.get(r.getParentId());
                    r.setPostId(parent);
                }

                // save the mapping for this comment or reply
                Integer id = activity.getId();
                if(!childToParentMappings.containsKey(id)) {
                    childToParentMappings.put(id, parent);
                }
            }
        }

        System.out.println("Resolved all mappings.");
    }


    public static void main(String[] args) throws Exception {
        Task3Evaluator evaluator = new Task3Evaluator("expected-anomalies.txt");
        evaluator.run();
    }
}
