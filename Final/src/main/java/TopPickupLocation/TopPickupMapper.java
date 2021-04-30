package TopPickupLocation;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TopPickupMapper extends Mapper<LongWritable, Text, TopPickupLocation.CompositeKey, DoubleWritable> {
	private TopPickupLocation.CompositeKey ck = new TopPickupLocation.CompositeKey();
	private DoubleWritable distance = new DoubleWritable();

	//location - distance
	
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
       
    	String[] line = value.toString().split(",");
    	try {
    	    if(key.toString().equals("0")) return;

            double tip = Double.parseDouble(line[14]);
            double total = Double.parseDouble(line[17]);

            if(tip > 0 && total > 0) {
                double tipPercent = tip / total;
                ck.set(line[8], String.format("%.6f", tipPercent));
                distance = new DoubleWritable(Double.parseDouble(line[5]));
                context.write(ck, distance);
            }
        } catch (Exception e) {
    	    //skip header line
        }
    }

}
