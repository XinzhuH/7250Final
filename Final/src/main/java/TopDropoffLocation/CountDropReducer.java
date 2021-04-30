package TopDropoffLocation;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CountDropReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        // count the frequency
        int count = 0;

        for (IntWritable val : values) {
            count += val.get();
        }

        context.write(key, new IntWritable(count));
    }
}
