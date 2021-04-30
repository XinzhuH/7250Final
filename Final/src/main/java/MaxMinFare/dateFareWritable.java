package MaxMinFare;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class dateFareWritable {
    private Text date;
    private DoubleWritable totalFare;

    public dateFareWritable() {
        date = new Text();
        totalFare = new DoubleWritable(0);
    }

    public dateFareWritable(Text date, DoubleWritable totalFare) {
        this.date = date;
        this.totalFare = totalFare;
    }

    public void set(Text date, DoubleWritable totalFare) {
        this.date = date;
        this.totalFare = totalFare;
    }

    public Text getDate() { return date; }

    public void setDate(Text date) { this.date = date; }

    public DoubleWritable getTotalFare() { return totalFare; }

    public void setTotalFare(DoubleWritable totalFare) { this.totalFare = totalFare; }

    public void readFields(DataInput in) throws IOException {
        date.readFields(in);
        totalFare.readFields(in);
    }
    public void write(DataOutput out) throws IOException {
        date.write(out);
        totalFare.write(out);

    }

    @Override
    public String toString() {
        return date.toString() + ":\t" + totalFare.toString();
    }
}
