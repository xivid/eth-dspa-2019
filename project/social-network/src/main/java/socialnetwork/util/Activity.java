package socialnetwork.util;

import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public abstract class Activity {
    final static Logger logger = LoggerFactory.getLogger("SocialNetwork");

    Integer personId;
    Integer postId = -1;
    LocalDateTime creationDate;
    Long eventTimestamp;

    void setEventTime(String s) {
        this.creationDate = LocalDateTime.from(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss[.S][S][S][X][X]").parse(s));
        this.eventTimestamp = creationDate.atZone(ZoneId.of("GMT+0")).toInstant().toEpochMilli();
    }

    public Long getEventTimestamp() {
        return eventTimestamp;
    }

    public Integer getPersonId() {
        return personId;
    }

    public static Activity fromString(String line) {
        if (line.startsWith("L|"))
            return new Like(line);
        if (line.startsWith("C|"))
            return Comment.fromString(line);
        if (line.startsWith("P|"))
            return new Post(line);
        if (line.startsWith("T|"))
            return new Tombstone(line);
        logger.error("Cannot create Activity of unknown type from {}", line);
        return null;
    }

    public Integer getKey() {
        return postId;
    }


    public Integer getPostId() {
        if (postId < 0) {
            logger.error("Getting unresolved postId of " + this.toString());
        }
        return postId;
    }

    public boolean isCommentOrReply() {
        return this instanceof Comment;  // Reply is also a subclass of Comment
    }

    public static class Deserializer extends AbstractDeserializationSchema<Activity> {
        @Override
        public Activity deserialize(byte[] bytes){
            return Activity.fromString(new String(bytes));
        }

        @Override
        public boolean isEndOfStream(Activity nextElement) {
            if (nextElement instanceof Tombstone){
                logger.info("Deserializer got Tombstone -> End of stream reached");
                return true;
            }
            return false;
        }
    }

    /**
     Format:
     P|id|personId|creationDate|imageFile|locationIP|browserUsed|language|content|tags|forumId|placeId
     */
    public static class Post extends Activity {
        String imageFile;
        String locationIP;
        String browserUsed;
        String language;
        String content;
        String tags;  // This is actually a list, e.g. "[5183, 1912, 778, 545]"
        Integer forumId;
        Integer placeId;

        Post(String line) {
            String[] splits = line.split("\\|");
            this.postId = Integer.valueOf(splits[1]);
            this.personId = Integer.valueOf(splits[2]);
            setEventTime(splits[3]);
            this.imageFile = splits[4];
            this.locationIP = splits[5];
            this.browserUsed = splits[6];
            this.language = splits[7];
            this.content = splits[8];
            this.tags = splits[9];
            this.forumId = Integer.valueOf(splits[10]);
            this.placeId = Integer.valueOf(splits[11]);
        }
    }

    /**
     Format:
     C|id|personId|creationDate|locationIP|browserUsed|content|reply_to_postId|(empty)|placeId
     */
    public static class Comment extends Activity {
        Integer commentId;
        String locationIP;
        String browserUsed;
        String content;
        Integer placeId;

        Comment(String[] splits, Integer postId) {
            this.commentId = Integer.valueOf(splits[1]);
            this.personId = Integer.valueOf(splits[2]);
            setEventTime(splits[3]);
            this.locationIP = splits[4];
            this.browserUsed = splits[5];
            this.content = splits[6];
            this.postId = postId;
            this.placeId = Integer.valueOf(splits[9]);
        }

        Comment(String[] splits) {
            this(splits, Integer.valueOf(splits[7]));
        }

        public static Comment fromString(String line) {
            String[] splits = line.split("\\|");
            if (splits[7].equals(""))  // no reply_to_postId
                return new Reply(splits);
            return new Comment(splits);
        }
    }

    /**
     Format:
     C|id|personId|creationDate|locationIP|browserUsed|content|(empty)|reply_to_commentId|placeId
     */
    public static class Reply extends Comment {
        Integer parentId;

        Reply(String[] splits) {
            super(splits, -1);
            this.parentId = Integer.valueOf(splits[8]);
        }

        public boolean isPostIdResolved() {
            return postId != -1;
        }

        @Override
        public Integer getKey() {
            System.out.println("overriden getkey");
            if (isPostIdResolved()) return postId;
            logger.info("Reply {} postId not resolved, use its own id as key", commentId);
            return commentId;
        }

    }

    /**
     Format:
     L|Person.id|Post.id|creationDate
     */
    public static class Like extends Activity {
        Like(String line) {
            String[] splits = line.split("\\|");
            this.personId = Integer.valueOf(splits[1]);
            this.postId = Integer.valueOf(splits[2]);
            setEventTime(splits[3]);
        }
    }

    /**
     Format:
     L|postId|creationDate
     */
    public static class Tombstone extends Activity {
        Tombstone(String line) {
            String[] splits = line.split("\\|");
            this.postId = Integer.valueOf(splits[1]);
            setEventTime(splits[2]);
        }
    }
}

