package socialnetwork.validation;

import socialnetwork.Producer;
import socialnetwork.util.Activity;
import socialnetwork.util.Activity.ActivityType;
import socialnetwork.util.Activity.Comment;
import socialnetwork.util.Activity.Reply;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static socialnetwork.util.Helpers.getFileWriter;


public class GenerateExpectedMappings {
    public static void main(String[] args) throws Exception {
        Map<String, String> mappings = resolvePostIdMappings();
        outputMappings("expected_mappings.txt", mappings);
    }

    public static Map<String, String> resolvePostIdMappings() throws Exception {
        System.out.println("Reading streams");
        TreeMap<Long, List<Activity>> data =
                Producer.readCleanedStreams(true, 0);

        Map<String, String> mappings = new HashMap<>();
        for (Map.Entry<Long, List<Activity>> entry : data.entrySet()) {
            // Add all relationships to the map
            for (Activity activity : entry.getValue()) {
                if (activity.getType() == ActivityType.Comment) {
                    int childId = ((Comment) activity).getId();
                    int parentId = ((Comment) activity).getParentId();
                    mappings.put("r_" + childId, "p_" + parentId);
                } else if (activity.getType() == ActivityType.Reply) {
                    long childId = ((Reply) activity).getId();
                    long parentId = ((Reply) activity).getParentId();
                    mappings.put("r_" + childId, "r_" + parentId);
                }
            }
        }

        // Resolve mappings
        for(Map.Entry<String, String> mapping : mappings.entrySet()) {
            String childId = mapping.getKey();
            String parentId = mapping.getValue();

            while(!parentId.startsWith("p_")) {
                parentId = mappings.get(parentId);
            }
            mappings.put(childId, parentId);
        }
        return mappings;
    }

    private static void outputMappings(String filename, Map<String, String> mappings) throws IOException {
        BufferedWriter out = getFileWriter(filename);

        for(Map.Entry<String, String> mapping : mappings.entrySet()) {
            String child = mapping.getKey();
            String parent = mapping.getValue();

            out.write(child + " -> " + parent);
            out.newLine();
        }
        out.close();

        System.out.println(String.format("Found %d mappings", mappings.size()));
    }
}
