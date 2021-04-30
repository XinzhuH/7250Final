package MaxMinFare;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class MaxFareDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        String path = "hdfs://localhost:9000";
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "maxFare");

        //input
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path(args[0]));

        job.setJarByClass(MaxFareDriver.class);
        job.setMapperClass(MaxMinFareMapper.class);
        job.setReducerClass(MaxFareReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(dateFareWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(dateFareWritable.class);

        //output
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean b = job.waitForCompletion(true);
        System.exit(b? 0 : 1);
    }
}
