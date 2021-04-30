package MaxMinFare;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MaxMinFareMapper extends Mapper<LongWritable, Text, Text, dateFareWritable>{
    private dateFareWritable dateFare = new dateFareWritable();

    private Text date = new Text();
    private DoubleWritable totalFare = new DoubleWritable(0);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split(",");
        try {
            date.set(line[2]);
            totalFare.set(Double.parseDouble(line[17]));
            dateFare.set(date, totalFare);

            //key: pickup location
            context.write(new Text(line[8]), dateFare);
        } catch(Exception e) {
            //skip header
        }

    }
}
