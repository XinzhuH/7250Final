package AvgPassenger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class AvgPassengerDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        String path = "hdfs://localhost:9000";
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "AvgPassenger");

        // input
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path(args[0]));

        // output
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setJarByClass(AvgPassengerDriver.class);
        job.setMapperClass(AvgPassengerMapper.class);
        job.setReducerClass(AvgPassengerReducer.class);
//        job.setCombinerClass(AvgPassengerCombiner.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);


        boolean b = job.waitForCompletion(true);
        System.exit(b? 0 : 1);

    }
}


//./hadoop jar /home/zzz/INFO7250/Final/Final/target/Final-1.0-SNAPSHOT.jar AvgPassenger.AvgPassengerDriver /final/mergedData.csv /finalOutput/AvgPassenger