package socialnetwork.validation;

import socialnetwork.Producer;
import socialnetwork.util.Activity;
import socialnetwork.util.Activity.ActivityType;
import socialnetwork.util.Activity.Reply;
import socialnetwork.util.Activity.Tombstone;

import java.util.*;

public class BatchPostIdResolver {
    private final Map<Integer, Integer> childToParentMappings;
    private final Map<Integer, List<Activity>> parentToChildMappings;

    private BatchPostIdResolver() {
        this.childToParentMappings = new HashMap<>();
        this.parentToChildMappings = new HashMap<>();
    }

    public static void main(String[] args) throws Exception {
        if(args.length <= 0) { return; }

        System.out.println("Reading streams");
        TreeMap<Long, List<Activity>> data =
                Producer.readCleanedStreams(true, 0);

        BatchPostIdResolver resolver = new BatchPostIdResolver();
        resolver.resolveMappings(data);

        Set<Integer> solved = new HashSet<>();
        for(String arg : args) {
            int postId = Integer.parseInt(arg);
            if(!solved.contains(postId)) {
                solved.add(postId);
                List<Activity> associatedMessages =
                        resolver.getAssociatedMessages(postId);
                StringBuilder sb = new StringBuilder("Post: ").append(postId).append("\n");
                for (Activity activity : associatedMessages) {
                    sb.append("\t").append(activity.toString()).append("\n");
                }
                System.out.println(sb.toString());
            }
        }
    }

    private List<Activity> getAssociatedMessages(int postId) {
        List<Activity> children = parentToChildMappings.get(postId);
        children.sort((t0, t1) ->
                t0.getCreationTimestamp() < t1.getCreationTimestamp() ?
                        -1 : t0.getCreationTimestamp().equals(t1.getCreationTimestamp()) ? 0 : 1);
        return children;
    }

    private void resolveMappings(final SortedMap<Long,  List<Activity>> data) {
        for(List<Activity> list : data.values()) {
            for(Activity activity : list) {
                if(activity instanceof Tombstone) {
                    continue;
                }

                Integer id = activity.getId();

                if(activity.getType() == ActivityType.Post) {
                    parentToChildMappings.put(id, new ArrayList<>());
                    parentToChildMappings.get(id).add(activity);
                    continue;
                }

                if(activity.getType() == ActivityType.Like) {
                    parentToChildMappings.get(id).add(activity);
                    continue;
                }

                Integer parent = activity.getPostId();

                /* reply is a special case */
                if(activity.getType() == ActivityType.Reply) {
                    Reply r = (Reply) activity;
                    parent = childToParentMappings.get(r.getParentId());
                }

                /* also add the reply as comment in the hashmap, in case
                 of reply of reply */
                if(!childToParentMappings.containsKey(id)) {
                    childToParentMappings.put(id, parent);
                }

                parentToChildMappings.get(parent).add(activity);
            }
        }
    }

}
