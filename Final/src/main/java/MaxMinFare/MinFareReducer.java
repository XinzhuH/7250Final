package MaxMinFare;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MinFareReducer extends Reducer<Text, dateFareWritable, Text, dateFareWritable> {
    dateFareWritable result = new dateFareWritable();

    @Override
    protected void reduce(Text key, Iterable<dateFareWritable> values, Context context) throws IOException, InterruptedException {
        double min = 999999999;
        for (dateFareWritable value : values) {
            double val = value.getTotalFare().get();
            if (val < min) {
                min = val;
                result = value;
            }
        }
        context.write(key, result);
    }
}
