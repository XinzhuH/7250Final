package TopDropoffLocation;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CountDropMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        if(key.toString().equals("0")) return;

        String[] line = value.toString().split(",");
        // dropoff location - line[9]
        context.write(new Text(line[9]), new IntWritable(1));
    }

}
