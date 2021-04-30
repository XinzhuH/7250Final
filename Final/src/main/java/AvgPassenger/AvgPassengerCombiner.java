package AvgPassenger;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AvgPassengerCombiner extends Reducer<Text, IntWritable, Text, DoubleWritable> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws
            IOException, InterruptedException {
        // combiner for each class is almost same only output value may be change as per requirement
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }

        context.write(key, new DoubleWritable(sum));
    }
}