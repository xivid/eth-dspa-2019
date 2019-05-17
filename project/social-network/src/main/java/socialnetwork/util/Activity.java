package socialnetwork.util;

import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

// Data type for Activities
public class Activity {

    public enum ActivityType {
        Post,
        Comment,
        Reply,
        Like,
        Tombstone,
        Others;

        static ActivityType fromString(String s) {
            if (s.equals("Post")) return Post;
            if (s.equals("Comment")) return Comment;
            if (s.equals("Like")) return Like;
            return Others;
        }
    };

    public ActivityType type;
    public Long ownId;  // TODO : refactor the fields with sub-types
    public Long postId;
    public Long userId;
    public LocalDateTime eventTime;
    public Long timestamp;

    public Activity(String line) {
        String[] splits = line.split(",");
        this.type = ActivityType.fromString(splits[0]);
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

    public Long getId() {
        switch (type) {
            case Post:
            case Like:
                return postId;
            case Comment:  // TODO for Comment: return postId or ownId?
            case Reply:
                return ownId;
            default:
                // TODO log error
                return -1L;
        }
    }

    public boolean isCommentOrReply() {
        return type == ActivityType.Comment || type == ActivityType.Reply;
    }

    public static class Deserializer extends AbstractDeserializationSchema<Activity> {
        @Override
        public Activity deserialize(byte[] bytes){
            return new Activity(new String(bytes));
        }

        @Override
        public boolean isEndOfStream(Activity nextElement) {
            if (nextElement.type == ActivityType.Tombstone){
                System.out.println("End of stream reached");  // TODO use log4j2
                return true;
            }
            return false;
        }
    }
}