package socialnetwork.validation;

import socialnetwork.util.Activity;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class TestJava {
    public static void main(String[] args) {
        String s = "2012-02-02T06:00:09Z";
//        s = "2012-02-02T10:32:46ZZ";
//        s = "2012-02-05T02:44:01.000ZZ";
//        s = "2012-02-05T02:44:01.00ZZ";
//        s = "2012-02-05T02:44:01.0ZZ";
        s =  "9999-12-31T23:59:59Z";
        LocalDateTime creationDate = LocalDateTime.from(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss[.S][S][S][X][X]").parse(s));
        Long eventTimestamp = creationDate.atZone(ZoneId.of("GMT+0")).toInstant().toEpochMilli();

        System.out.println(creationDate);
        System.out.println(eventTimestamp);

        Activity c = Activity.fromString("C|1045520|31|2012-02-02T09:46:28ZZ|192.132.35.23|Firefox|About Vladimir Lenin, 1917. As leader of the Bolsheviks, he headed the Soviet. About Kurt Weill, States. He was a leading composer for the stage who was.||1045500|11");
        System.out.println(c.getPostId());
        System.out.println(c.getKey());
        System.out.println(c.getClass());
        Activity.Reply r = (Activity.Reply) c;
        r.setPostId(12);
        System.out.println(c.getPostId());
        System.out.println(c.getKey());

        s = "2012-02-09 00:12:17.000ZZ";
        creationDate = LocalDateTime.from(DateTimeFormatter.ofPattern("yyyy-MM-dd' 'HH:mm:ss[.S][S][S][X][X]").parse(s));
        System.out.println(creationDate);

    }
}
