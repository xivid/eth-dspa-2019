package socialnetwork.cleaning;

import org.apache.flink.api.java.tuple.Tuple3;
import socialnetwork.util.Config;
import socialnetwork.util.Helpers;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import static socialnetwork.util.Activity.*;

class StreamsCleaner {
    // for printing
    private final TreeMap<Long, List<Comment>> commentTimes = new TreeMap<>();
    // comm_id, <reference, is_post, comm>
    private final Map<Integer, Tuple3<Integer, Boolean, Comment>> tree = new HashMap<>();
    private final Map<Integer, Post> posts = new HashMap<>();
    private final Map<Integer, List<Comment>> waitingFor = new HashMap<>();
    private final SimpleDateFormat sdf;

    private StreamsCleaner() {
        sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
    }

    private void addCommentTime(Long key, Comment m){
        if(!commentTimes.containsKey(key)){
            commentTimes.put(key, new ArrayList<>());
        }
        commentTimes.get(key).add(m);
    }

    private void generate(){
        System.out.println("Working Directory = " + System.getProperty("user.dir"));
        String[] rawFilesNames = Config.getStreamRawInputFiles();
        final BufferedReader commentsInputFile = Helpers.getFileReader(rawFilesNames[0]);
        final BufferedReader likesInputFile = Helpers.getFileReader(rawFilesNames[1]);
        final BufferedReader postsInputFile = Helpers.getFileReader(rawFilesNames[2]);

        String[] cleanedFileNames = Config.getStreamCleanedInputFiles();
        final BufferedWriter commentsOutputFile = Helpers.getFileWriter(cleanedFileNames[0]);
        final BufferedWriter likesOutputFile = Helpers.getFileWriter(cleanedFileNames[1]);
        final BufferedWriter postsOutputFile = Helpers.getFileWriter(cleanedFileNames[2]);

        assert (commentsInputFile != null);
        assert (postsInputFile != null);
        assert (likesInputFile != null);
        assert (commentsOutputFile != null);
        assert (postsOutputFile != null);
        assert (likesOutputFile != null);

        String line;
        System.out.println("Sorting cleaning...");

//        try {
//            commentsOutputFile.write("id|personId|creationDate|locationIP|browserUsed|content|reply_to_postId|reply_to_commentId|placeId\n");
//            likesOutputFile.write("Person.id|Post.id|creationDate\n");
//            postsOutputFile.write("id|personId|creationDate|imageFile|locationIP|browserUsed|language|content|tags|forumId|placeId\n");
            // TODO
//
//            // add all posts
//            postsInputFile.readLine();
//            while ((line = postsInputFile.readLine()) != null) {
//                Post p = (Post) Post.fromString("P|" + line);
//                posts.put(p.getId(), p);
//                postsOutputFile.write(line);
//                postsOutputFile.newLine();
//            }
//            postsInputFile.close();
//
//            TreeMap<Long, List<Like>> likeTimes = new TreeMap<>();
//
//            // add all likes, fix if they come later
//            likesInputFile.readLine();
//            while ((line = likesInputFile.readLine()) != null) {
//                Like like = (Like) Like.fromString("L|" + line);
//                int postId = like.getPostId();
//                Post post = posts.get(postId);
//                long likeDate = like.getCreationTimestamp();
//                long postDate = post.getCreationTimestamp();
//                if(postDate >= likeDate) { // post later than like
//                    long diff = postDate - likeDate;
//                    if(diff == 0) {
//                        diff = Time.seconds(10).toMilliseconds();
//                    }
//                    likeDate += 2 * diff;
//                    like.setCreationDate(sdf.format(likeDate).replace(" ","T") + ".000Z");
//                }
//                if(!likeTimes.containsKey(likeDate)) {
//                    likeTimes.put(likeDate, new ArrayList<>());
//                }
//                likeTimes.get(likeDate).add(like);
//            }
//            likesInputFile.close();
//
//            for(Long timestamp : likeTimes.keySet()) {
//                List<Like> m = likeTimes.get(timestamp);
//                for(Like l : m) {
//                    l.setCreationDate(sdf.format(l.getCreationTimestamp()).replace(" ","T") + ".000ZZ");
//                    likesOutputFile.write(l.getRawString());
//                    likesOutputFile.newLine();
//                }
//            }
//            likeTimes = null;
//
//            // add all comments
//            commentsInputFile.readLine();
//            while ((line = commentsInputFile.readLine()) != null) {
//                final Comment comment = Comment.fromString("C|" + line);
//                if(!comment.isReply()) { // comment, adjust date and add to tree
//                    int postId = comment.getPostId();
//                    Post post = posts.get(postId);
//                    long commentDate = comment.getCreationTimestamp();
//                    long postDate = post.getCreationTimestamp();
//                    if(postDate >= commentDate) { // post later than comm
//                        long diff = postDate - commentDate;
//                        if(diff == 0) {
//                            diff = Time.seconds(10).toMilliseconds();
//                        }
//                        commentDate += 2 * diff;
//                        comment.setCreationDate(sdf.format(commentDate));
//                    }
//
//                    tree.put(comment.getId(), new Tuple3<>(postId, true, comment));
//                    addCommentTime(commentDate, comment);
//                    recursiveFix(comment.getId(), commentDate);
//                } else {
//                    final Reply reply = (Reply) comment;
//                    final int parentId = reply.getParentId();
//                    if(tree.containsKey(parentId)){
//                        Tuple3<Integer, Boolean, Comment> t = tree.get(parentId);
//                        Comment parent = t.f2;
//                        long replyDate = reply.getCreationTimestamp();
//                        long parentDate = parent.getCreationTimestamp();
//                        if(parentDate >= replyDate) { // post later than comm
//                            long diff = parentDate - replyDate;
//                            if(diff == 0) {
//                                diff = Time.seconds(10).toMilliseconds();
//                            }
//                            replyDate += 2 * diff;
//                            reply.setCreationDate(sdf.format(replyDate));
//                        }
//
//                        tree.put(reply.getId(), new Tuple3<>(parentId, false, reply));
//                        addCommentTime(replyDate, reply);
//                        recursiveFix(reply.getId(), replyDate);
//                    } else { // parent is not there, reply out of order
//                        if(!waitingFor.containsKey(parentId)) {
//                            waitingFor.put(parentId, new ArrayList<>());
//                        }
//                        waitingFor.get(parentId).add(reply);
//                    }
//                }
//            }
//            commentsInputFile.close();
//            assert waitingFor.size() == 0;
//
//            // use commentTimes to create all strings
//            for(Long timestamp : commentTimes.keySet()) {
//                List<Comment> m = commentTimes.get(timestamp);
//                for(Comment c : m) {
//                    c.setCreationDate(sdf.format(c.getCreationTimestamp()).replace(" ","T") + "ZZ");
//                    commentsOutputFile.write(c.getRawString());
//                    commentsOutputFile.newLine();
//                }
//            }
//
//            System.out.println("Processed, writing into file");
//
//            commentsOutputFile.flush();
//            commentsOutputFile.close();
//            postsOutputFile.flush();
//            postsOutputFile.close();
//            likesOutputFile.flush();
//            likesOutputFile.close();
//
//            System.out.println("Done");
//        } catch (IOException | ParseException e){
//            e.printStackTrace();
//        }
    }

    public static void main(String[] args) {
        new StreamsCleaner().generate();
        new OrderedFileChecker().checkTree();
    }
}

