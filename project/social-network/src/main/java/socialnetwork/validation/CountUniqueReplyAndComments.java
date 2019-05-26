package socialnetwork.validation;

import socialnetwork.util.Activity.Comment;
import socialnetwork.util.Config;

import java.io.*;
import java.util.*;

import static socialnetwork.util.Helpers.getFileWriter;

public class CountUniqueReplyAndComments {

    public static void main(String[] args) throws Exception {
        final TreeMap<Long, ArrayList<Comment>> data = new TreeMap<>();
        final BufferedReader reader =
                new BufferedReader(new InputStreamReader(
                        new FileInputStream(new File(Config.Comments_1K))));
        reader.readLine(); // avoid header
        String line;
        while ((line = reader.readLine()) != null) {
            Comment record =
                    Comment.fromString("C|" + line);
            Long timestamp = record.getCreationTimestamp();
            if (!data.containsKey(timestamp)) {
                data.put(timestamp, new ArrayList<>());
            }
            data.get(timestamp).add(record);
        }

        Set<Integer> uniqueIds = new HashSet<>();

        for(Map.Entry<Long, ArrayList<Comment>> comments : data.entrySet()) {
            for(Comment comment : comments.getValue()) {
                int childId = comment.getId();
                if(uniqueIds.contains(childId)) {
                    System.out.println("DUPLICATE FOUND: " + childId);
                }
                uniqueIds.add(childId);
            }
        }

        BufferedWriter out = getFileWriter("ids.txt");

        for(Integer id : uniqueIds) {
            out.write(id.toString());
            out.newLine();
        }
        out.close();
        System.out.println(uniqueIds.size());
    }

}
