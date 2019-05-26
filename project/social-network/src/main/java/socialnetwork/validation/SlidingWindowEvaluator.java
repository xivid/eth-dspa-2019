package socialnetwork.validation;

import org.apache.flink.api.common.time.Time;
import socialnetwork.Producer;
import socialnetwork.util.Activity;

import java.util.*;

public abstract class SlidingWindowEvaluator {
    static final long SECOND = 1000;
    static final long MINUTE = 60 * SECOND;
    static final long HOUR = 60 * MINUTE;

    /* Window configuration */
    long size = Time.hours(12).toMilliseconds();
    long slide = Time.minutes(30).toMilliseconds();

    /* Current window */
    long currentStart = 0L;
    long currentEnd = 0L; // non-inclusive

    private void setupWindow(long value){
        currentEnd = value + slide - (value % slide);
        currentStart = currentEnd - size;
    }

    abstract void processWindow(List<Activity> window);

    private void slideWindow(List<Activity> window) {
        currentEnd += slide;
        currentStart += slide;
        window.removeIf(activity -> activity.getCreationTimestamp() < currentStart);
    }

    public void run() throws Exception {
        LinkedList<Activity> window = new LinkedList<>();

        // get stream of activities and resolve the mappings
        TreeMap<Long, List<Activity>> data = Producer.readCleanedStreams(true, 0);
        resolveMappings(data);

        while (!data.isEmpty()) {
            // forward to the earliest non-empty window
            setupWindow(data.firstKey());

            // slide until no activities remain
            do {
                // collect everything within the window
                while (!data.isEmpty() && data.firstKey() < currentEnd) {
                    List<Activity> activities = data.pollFirstEntry().getValue();
                    window.addAll(activities);
                }
                processWindow(window);
                slideWindow(window);
            } while (!window.isEmpty());

            System.out.println(String.format("Window [%d, %d) has no overlapped input, fast forward", currentStart, currentEnd));
        }
    }

    private void resolveMappings(final SortedMap<Long,  List<Activity>> data) {
        final Map<Integer, Integer> childToParentMappings;
        childToParentMappings = new HashMap<>();

        for(List<Activity> list : data.values()) {
            for(Activity activity : list) {
                if(activity instanceof Activity.Tombstone) {
                    continue;
                }

                // Skip posts, because post ids overlap with comment ids
                if(activity.getType() == Activity.ActivityType.Post) {
                    continue;
                }

                // Skip likes, because post ids overlap with comment ids
                if(activity.getType() == Activity.ActivityType.Like) {
                    continue;
                }

                Integer parent = activity.getPostId();

                // update postid field for replies
                if(activity.getType() == Activity.ActivityType.Reply) {
                    Activity.Reply r = (Activity.Reply) activity;
                    parent = childToParentMappings.get(r.getParentId());
                    r.setPostId(parent);
                }

                // save the mapping for this comment or reply
                Integer id = activity.getId();
                if(!childToParentMappings.containsKey(id)) {
                    childToParentMappings.put(id, parent);
                }
            }
        }

        System.out.println("Resolved all mappings.");
    }
}
