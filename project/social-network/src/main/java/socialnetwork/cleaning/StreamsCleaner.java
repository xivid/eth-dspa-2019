package socialnetwork.cleaning;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import socialnetwork.util.Config;
import socialnetwork.util.Helpers;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static socialnetwork.util.Activity.*;

class StreamsCleaner {

    private static void generate() throws IOException {
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

        System.out.println(String.format("Creating ordered files for %s users...", Config.use1KFiles ? "1k" : "10k"));
        String line;
        System.out.println("Reading Posts...");
        final Map<Integer, Long> postMap = new HashMap<>(100000);

        postsInputFile.readLine();
        postsOutputFile.write("id|personId|creationDate|imageFile|locationIP|browserUsed|language|content|tags|forumId|placeId\n");
        while ((line = postsInputFile.readLine()) != null) {
            String[] fields = line.split("\\|");
            int postId = Integer.parseInt(fields[0]);
            long timestamp = dateToTimestamp(fields[2]);
            postMap.put(postId, timestamp);
            postsOutputFile.write(line);
            postsOutputFile.newLine();
        }
        postsOutputFile.flush();
        postsOutputFile.close();
        postsInputFile.close();

        System.out.println("Created postMap.");

        System.out.println("Fixing likes...");
        likesInputFile.readLine();
        likesOutputFile.write("Person.id|Post.id|creationDate\n");
        int incorrectLikes = 0;
        while ((line = likesInputFile.readLine()) != null) {
            String[] fields = line.split("\\|");
            int postId = Integer.parseInt(fields[1]);
            long likeTimestamp = dateToTimestamp(fields[2]);
            long postTimestamp = postMap.get(postId);
            if(postTimestamp >= likeTimestamp) {
                incorrectLikes++;
                continue;
            }
            likesOutputFile.write(line);
            likesOutputFile.newLine();
        }
        System.out.println("Num incorrect likes = " + NumberFormat.getNumberInstance(Locale.UK).format(incorrectLikes));
        likesOutputFile.flush();
        likesOutputFile.close();
        likesInputFile.close();
        System.out.println("Finished fixing likes.");

        final Map<String, String> mappings = new HashMap<>();
        final Set<String> parents = new HashSet<>();
        final Map<String, Long> commentTimes = new HashMap<>();

        commentsInputFile.readLine();
        commentsOutputFile.write("id|personId|creationDate|locationIP|browserUsed|content|reply_to_postId|reply_to_commentId|placeId\n");
        int lineCount = 0;
        while ((line = commentsInputFile.readLine()) != null) {
            if(++lineCount % 100000 == 0) {
                System.out.println("lines read = " + NumberFormat.getNumberInstance(Locale.UK).format(lineCount));
                commentsOutputFile.flush();
            }

            String[] fields = line.split("\\|");
            String childId = String.format("r_%d", Integer.parseInt(fields[0]));
            long childTimestamp = dateToTimestamp(fields[2]);

            String parentId;
            if(fields[6].equals("")) {
                parentId = String.format("r_%s", fields[7]); // This is a reply
            } else {
                parentId = String.format("p_%s", fields[6]);; // This is a comment
            }
            mappings.put(childId, parentId);
            commentTimes.put(childId, childTimestamp);
            parents.add(parentId);
        }

        System.out.println("lines read = " + NumberFormat.getNumberInstance(Locale.UK).format(lineCount));

        final Map<String, Set<String>> commentMap = new HashMap<>(1000000);
        final Set<String> toBeDeleted = new HashSet<>(1000000);

        Set<String> leafIds = new HashSet<>(mappings.keySet());
        leafIds.removeAll(parents);

        for(String id : leafIds) {
            String currentKey = id;
            String prevKey;
            final Set<String> traversalIds = new HashSet<>();
            boolean violatedOrdering = false;
            do {
                traversalIds.add(currentKey);
                prevKey = currentKey;
                if(!mappings.containsKey(currentKey)) {
                    System.out.println(currentKey);
                }
                currentKey = mappings.get(currentKey);
                assert currentKey != null;
                assert prevKey != null;

                long prevKeyTimestamp = commentTimes.get(prevKey);
                long currentKeyTimestamp;

                if(currentKey.startsWith("p_")) {
                    int key = Integer.parseInt(currentKey.substring(2));
                    currentKeyTimestamp = postMap.get(key);
                } else {
                    if(!commentTimes.containsKey(currentKey)) {
                        System.out.println("Comment times does not contain key = " + currentKey);
                    }
                    currentKeyTimestamp = commentTimes.get(currentKey);
                }

                if(currentKeyTimestamp > prevKeyTimestamp) {
                    violatedOrdering = true;
                }
            } while(!currentKey.startsWith("p_"));

            // currentKey == post_id
            // prevKey == comment_id
            commentMap.putIfAbsent(prevKey, new HashSet<>());
            commentMap.get(prevKey).addAll(traversalIds);
            if(violatedOrdering) {
                toBeDeleted.add(prevKey);
            }
        }

        Set<String> toDelete = new HashSet<>();
        for(String commentId : toBeDeleted) {
            toDelete.add(commentId);
            toDelete.addAll(commentMap.get(commentId));
        }

        commentsInputFile.readLine();
        commentsOutputFile.write("id|personId|creationDate|locationIP|browserUsed|content|reply_to_postId|reply_to_commentId|placeId\n");
        int deleted = 0;
        lineCount = 0;
        while ((line = commentsInputFile.readLine()) != null) {
            if(++lineCount % 100000 == 0) {
                System.out.println("lines read = " + NumberFormat.getNumberInstance(Locale.UK).format(lineCount));
                commentsOutputFile.flush();
            }

            String[] fields = line.split("\\|");
            String childId = String.format("r_%d", Integer.parseInt(fields[0]));

            if(toDelete.contains(childId)) {
                if(++deleted % 10000 == 0) {
                    System.out.println("Deleted = " + NumberFormat.getNumberInstance(Locale.UK).format(deleted));
                }
                continue;
            }
            commentsOutputFile.write(line);
            commentsOutputFile.newLine();
        }

        System.out.println("lines read = " + NumberFormat.getNumberInstance(Locale.UK).format(lineCount));
        System.out.println("Deleted = " + NumberFormat.getNumberInstance(Locale.UK).format(deleted));
        commentsOutputFile.flush();
        commentsOutputFile.close();
        commentsInputFile.close();
    }

    private static long dateToTimestamp(String date) {
        return LocalDateTime
                .from(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss[.S][S][S][X][X]").parse(date))
                .atZone(ZoneId.of("GMT+0")).toInstant().toEpochMilli();
    }

    public static void main(String[] args) throws IOException {
        StreamsCleaner.generate();
        OrderedFileChecker.checkTree();
    }
}

