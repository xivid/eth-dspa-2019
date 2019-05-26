package socialnetwork.util;

import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public abstract class Activity {
    final static Logger logger = LoggerFactory.getLogger("SocialNetwork");

    public enum ActivityType {
        Post,
        Comment,
        Reply,
        Like,
        Tombstone;

        static ActivityType fromString(String s) {
            if (s.equals("Post")) return Post;
            if (s.equals("Comment")) return Comment;
            if (s.equals("Reply")) return Reply;
            if (s.equals("Like")) return Like;
            return Tombstone;
        }
    }

    public ActivityType getType() {
        if (this instanceof Post) return ActivityType.Post;
        if (this instanceof Reply) return ActivityType.Reply;
        if (this instanceof Comment) return ActivityType.Comment;  // Comment but not Reply
        if (this instanceof Like) return ActivityType.Like;
        return ActivityType.Tombstone;
    }

    // Common fields of all activities
    Integer personId;
    Integer postId = -1;
    String creationDate;
    Long creationTimestamp;

    public void setCreationDate(String s) {
        // Ref: https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html#patterns
        this.creationDate = s;
        this.creationTimestamp = LocalDateTime
                .from(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss[.S][S][S][X][X]").parse(s))
                .atZone(ZoneId.of("GMT+0")).toInstant().toEpochMilli();
    }

    public String getCreationDate() { return creationDate; }

    public Long getCreationTimestamp() {
        return creationTimestamp;
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

    public abstract Integer getId();

    public boolean isCommentOrReply() {
        return this instanceof Comment;  // Reply is also a subclass of Comment
    }

    public boolean isReply() {
        return this instanceof Reply;
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

    public abstract String getRawString();

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
            setCreationDate(splits[3]);
            this.imageFile = splits[4];
            this.locationIP = splits[5];
            this.browserUsed = splits[6];
            this.language = splits[7];
            this.content = splits[8];
            this.tags = splits[9];
            this.forumId = Integer.valueOf(splits[10]);
            this.placeId = Integer.valueOf(splits[11]);
        }

        public String toString() {
            return "P|" + postId + "|" + personId + "|" + creationDate + "|" + imageFile + "|" + locationIP + "|"
                    + browserUsed + "|" + language + "|" + content + "|" + tags + "|" + forumId + "|" + placeId;
        }

        public Integer getId() {
            return postId;
        }

        public String getContent() { return content; }

        @Override
        public String getRawString() {
            String s = this.toString();
            return s.substring(2);
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
            setCreationDate(splits[3]);
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

        public Integer getId() {
            return commentId;
        }

        public Integer getParentId() {
            return postId;
        }

        public String getContent() { return content; }

        public String toString() {
             return "C|" + commentId + "|" + personId + "|" + creationDate + "|" + locationIP + "|" + browserUsed
                     + "|" + content + "|" + postId + "||" + placeId;
        }

        @Override
        public String getRawString() {
            String s = this.toString();
            return s.substring(2);
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
            if (isPostIdResolved()) return postId;
//            logger.debug("Reply {} postId not resolved, use its own id as key", commentId);
            return commentId;
        }

        @Override
        public Integer getParentId() {
            return parentId;
        }

        public void setPostId(Integer val) {
            if (postId != -1)
                logger.warn("Overwriting already resolved postId {} with {}!", postId, val);
            postId = val;
        }

        public String toString() {
            return "C|" + commentId + "|" + personId + "|" + creationDate + "|" + locationIP + "|" + browserUsed
                    + "|" + content + "|" + (isPostIdResolved() ? postId : "") + "|" + parentId
                    + "|" + placeId;
        }

        @Override
        public String getRawString() {
            String s = this.toString();
            return s.substring(2);
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
            setCreationDate(splits[3]);
        }

        public String toString() {
            return "L|" + personId + "|" + postId + "|" + creationDate;
        }

        public Integer getId() {
            return postId;
        }

        @Override
        public String getRawString() {
            String s = this.toString();
            return s.substring(2);
        }
    }

    /**
     Format:
     T|postId|creationDate
     */
    public static class Tombstone extends Activity {
        public Tombstone(Integer postId, String creationDate) {
            this.postId = postId;
            setCreationDate(creationDate);
        }

        Tombstone(String line) {
            String[] splits = line.split("\\|");
            this.postId = Integer.valueOf(splits[1]);
            setCreationDate(splits[2]);
        }

        public String toString() {
            return "T|" + postId + "|" + creationDate;
        }

        @Override
        public String getRawString() {
            String s = this.toString();
            return s.substring(2);
        }

        public Integer getId() {
            return postId;
        }
    }
}

