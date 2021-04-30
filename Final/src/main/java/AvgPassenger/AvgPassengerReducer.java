package AvgPassenger;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AvgPassengerReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int sumP = 0; // sum of passenger
        int sumT = 0; // sum of trip

        for (IntWritable val : values) {
            sumP += val.get();
            sumT += 1;
        }

        double avgP = sumP / sumT;

        context.write(key, new DoubleWritable(avgP));
    }
}
