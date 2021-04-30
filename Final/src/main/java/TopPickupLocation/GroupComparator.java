package TopPickupLocation;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


public class GroupComparator extends WritableComparator {

    protected GroupComparator() {
        super(TopPickupLocation.CompositeKey.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        return -1 * ((TopPickupLocation.CompositeKey) a).getLocation().compareTo(((TopPickupLocation.CompositeKey) b).getLocation());
    }
}
