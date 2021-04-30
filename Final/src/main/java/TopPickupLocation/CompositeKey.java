package TopPickupLocation;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CompositeKey implements WritableComparable<CompositeKey> {

    public String location;
    public String tipPercent;


	public String getLocation() {
		return location;
	}

	public void setLocation(String location) { this.location = location; }

	public String getTipPercent() {
		return tipPercent;
	}

	public void setTipPercent(String tipPercent) {
		this.tipPercent = tipPercent;
	}
	
	public void set(String location, String tipPercent) {
		this.location = location;
		this.tipPercent= tipPercent;
	}


	public void write(DataOutput out) throws IOException {
		out.writeUTF(location);
		out.writeUTF(tipPercent);
    }

    public void readFields(DataInput in) throws IOException {
    	location = in.readUTF();
		tipPercent = in.readUTF();
    }

    @Override
    public String toString() {
        return location + "\t" + tipPercent + "\t";
    }

    public int compareTo(CompositeKey o) {
        if (location.compareTo(o.getLocation()) < 0) return -1;
        if (location.compareTo(o.getLocation()) > 0) return 1;
        return 0;
    }
}
