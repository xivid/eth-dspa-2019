package socialnetwork.validation;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import socialnetwork.util.Activity;
import socialnetwork.util.Activity.*;
import static socialnetwork.util.Helpers.*;

import java.io.BufferedWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

public class Task1Evaluator extends SlidingWindowEvaluator {

    /* <PostId, <personId>> */
    private HashMap<Integer, Set<Integer>> activeUsers = new HashMap<>();

    /* output-related */
    private BufferedWriter commentWriter;
    private BufferedWriter replyWriter;
    private BufferedWriter userWriter;

    public Task1Evaluator(String commentPath, String replyPath, String usersPath) {
        size = Time.hours(12).toMilliseconds();
        slide = Time.minutes(30).toMilliseconds();
        commentWriter = getFileWriter(commentPath);
        replyWriter = getFileWriter(replyPath);
        userWriter = getFileWriter(usersPath);
        dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
    }

    SimpleDateFormat dateFormat;
    private boolean output_users;

    void processWindow(List<Activity> window) {
        String windowFormat = "Start:\t" + dateFormat.format(currentStart) + "\tEnd:\t" + dateFormat.format(currentEnd) + "\n";
        StringBuilder commentsOutput = new StringBuilder();
        StringBuilder repliesOutput = new StringBuilder();
        StringBuilder usersOutput = new StringBuilder();

        TreeMap<Integer, Tuple2<Integer, Integer>> statistics = new TreeMap<>();  // postId -> (comments, replies)

        if (currentStart % HOUR == 0) {
            output_users = true;
            activeUsers = new HashMap<>();
        } else {
            output_users = false;
        }

        for(Activity activity : window) {
            Integer postId = activity.getPostId();

            // post id should always have been resolved
            assert postId != null;

            /* add to statistics */
            if (!statistics.containsKey(postId)) {
                statistics.put(postId, new Tuple2<>(0, 0));
            }

            /* update count */
            Tuple2<Integer, Integer> stats = statistics.get(postId);
            switch (activity.getType()) {
                case Comment:
                    stats.f0 += 1;
                    break;
                case Reply:
                    stats.f1 += 1;
                    break;
            }

            /* unique users count */
            if (output_users) {
                int person_Id = activity.getPersonId();
                if (!activeUsers.containsKey(postId)) {
                    activeUsers.put(postId, new HashSet<>());
                }
                activeUsers.get(postId).add(person_Id);
            }
        }

        /* To conclude, iterate over the hashmap to print statistics */
        for(Integer postId : statistics.keySet()) {
            Tuple2<Integer, Integer> stats = statistics.get(postId);

            Tuple3<Long, Integer, Integer> commentStats = Tuple3.of(currentEnd, postId, stats.f0);
            commentsOutput.append(commentStats.toString());
            commentsOutput.append("\n");

            Tuple3<Long, Integer, Integer> replyStats = Tuple3.of(currentEnd, postId, stats.f1);
            repliesOutput.append(replyStats.toString());
            repliesOutput.append("\n");

            if (output_users) {
                int numActiveUsers = activeUsers.get(postId).size();
                Tuple3<Long, Integer, Integer> usersStats = Tuple3.of(currentEnd, postId, numActiveUsers);
                usersOutput.append(usersStats.toString());
                usersOutput.append("\n");
            }
        }

        try {
            replyWriter.write(repliesOutput.toString());
            replyWriter.flush();
            commentWriter.write(commentsOutput.toString());
            commentWriter.flush();
            userWriter.write(usersOutput.toString());
            userWriter.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();

        try {
            commentWriter.close();
            replyWriter.close();
            userWriter.close();
        } catch (IOException e) {
            System.out.println("Failed to close file");
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        Task1Evaluator evaluator = new Task1Evaluator(
                "expected-comment-counts.txt",
                "expected-reply-counts.txt",
                "expected-user-counts.txt");
        evaluator.run();
    }
}
