package socialnetwork.cleaning;

import socialnetwork.util.Config;
import socialnetwork.util.Helpers;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static socialnetwork.util.Activity.*;

class OrderedFileChecker {
    // id, time
    private final Map<Integer, Long> posts = new HashMap<>();
    private final Map<Integer, Long> comments = new HashMap<>();

    void checkTree(){
        String[] cleanedFileNames = Config.getStreamCleanedInputFiles();
        final BufferedReader commR = Helpers.getFileReader(cleanedFileNames[0]);
        final BufferedReader likesR = Helpers.getFileReader(cleanedFileNames[1]);
        final BufferedReader postR = Helpers.getFileReader(cleanedFileNames[2]);

        assert (postR != null);
        assert (commR != null);
        assert (likesR != null);

        String line;
        System.out.println("Checking cleaning...");

        try {
            // add all posts
            postR.readLine();
            long old_time = 0;
            while ((line = postR.readLine()) != null) {
                Post p = (Post) Post.fromString("P|" + line);
                long curr_time = p.getCreationTimestamp();
                posts.put(p.getId(), curr_time);
                assert (curr_time >= old_time);
                old_time = curr_time;
            }

            // add all likes, fix if they come later
            likesR.readLine();
            old_time = 0;
            while ((line = likesR.readLine()) != null) {
                Like like = (Like) Like.fromString("L|" + line);
                int postId = like.getPostId();
                long post_date = posts.get(postId);
                long like_date = like.getCreationTimestamp();

                assert (post_date < like_date);
                assert (like_date >= old_time);
                old_time = like_date;
            }

            // add all comments
            commR.readLine();
            old_time = 0;
            while ((line = commR.readLine()) != null) {
                Comment comm = Comment.fromString("C|" + line);
                long comm_date = comm.getCreationTimestamp();

                if(!comm.isReply()) { // comment, adjust date and add to tree
                    int postId = comm.getPostId();
                    long post_date = posts.get(postId);
                    assert (post_date < comm_date);
                    comments.put(comm.getId(), comm_date);
                } else {
                    Reply reply = (Reply) comm;
                    int parent = reply.getParentId();
                    assert (comments.containsKey(parent));
                    long parent_date = comments.get(parent);
                    assert (parent_date < comm_date);
                    comments.put(reply.getId(), comm_date);
                }
                assert (comm_date >= old_time);
                old_time = comm_date;
            }

            System.out.println("Done, all correct");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
