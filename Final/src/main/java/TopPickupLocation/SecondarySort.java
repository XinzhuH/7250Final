package TopPickupLocation;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SecondarySort extends WritableComparator {

    protected SecondarySort() {
        super(CompositeKey.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        int cmp = ((CompositeKey) a).getLocation().compareTo(((CompositeKey) b).getLocation());
        if (cmp != 0) return cmp;

        return -1 * ((CompositeKey) a).getTipPercent().compareTo(((CompositeKey) b).getTipPercent());
    }

}
