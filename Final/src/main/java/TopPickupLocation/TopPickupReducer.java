package TopPickupLocation;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class TopPickupReducer extends Reducer<TopPickupLocation.CompositeKey, DoubleWritable, Text, DoubleWritable> {
	
	@Override
	protected void reduce(TopPickupLocation.CompositeKey key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

        for(DoubleWritable value: values) {
            if(value.get() > 10) {
                context.write(new Text(key.toString()), value);
            }
        }

    }

}
