package socialnetwork.util;

import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

// Data type for Activities, deprecated, not used anywhere
@Deprecated
public class ActivityOld {
    final static Logger logger = LoggerFactory.getLogger("SocialNetwork");

    public enum ActivityOldType {
        Post,
        Comment,
        Reply,
        Like,
        Tombstone,
        Others;

        static ActivityOldType fromString(String s) {
            if (s.equals("Post")) return Post;
            if (s.equals("Comment")) return Comment;
            if (s.equals("Like")) return Like;
            return Others;
        }
    };

    public ActivityOldType type;
    public Long ownId;  // TODO : refactor the fields with sub-types
    public Long postId;
    public Long userId;
    public LocalDateTime eventTime;
    public Long timestamp;

    public ActivityOld(String line) {
        String[] splits = line.split(",");
        this.type = ActivityOldType.fromString(splits[0]);
        this.postId = Long.valueOf(splits[1]);
        this.userId = Long.valueOf(splits[2]);
        this.eventTime = LocalDateTime.from(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").parse(splits[3]));
        this.timestamp = eventTime.atZone(ZoneId.of("GMT+0")).toInstant().toEpochMilli();
    }

    @Override
    public String toString() {
        return "(type: " + type + ", postId: " + postId + ", userId: " + userId
                + ", eventTime: " + eventTime + ", timestamp: " + timestamp + ")";
    }

    public Long getKey() {
        switch (type) {
            case Post:
            case Like:
            case Comment:  // TODO for Comment: return postId or ownId?
                return postId;
            case Reply:
                return ownId;
            default:
                // TODO log error
                return -1L;
        }
    }

    public Long getPostId() {
        if (postId < 0) {
            logger.error("Getting unresolved postId of " + this.toString());
        }
        return postId;
    }

    public boolean isCommentOrReply() {
        return type == ActivityOldType.Comment || type == ActivityOldType.Reply;
    }

    public static class Deserializer extends AbstractDeserializationSchema<ActivityOld> {
        @Override
        public ActivityOld deserialize(byte[] bytes){
            return new ActivityOld(new String(bytes));
        }

        @Override
        public boolean isEndOfStream(ActivityOld nextElement) {
            if (nextElement.type == ActivityOldType.Tombstone){
                System.out.println("End of stream reached");  // TODO use log4j2
                return true;
            }
            return false;
        }
    }
}