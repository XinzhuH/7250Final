package TopDropoffLocation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.*;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class TopDropLocationDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        String path = "hdfs://localhost:9000";
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "ChainJobTopDrop");

        // input
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path(args[0]));

        // output
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        // mapper 1
        ChainMapper.addMapper(job,
                CountDropMapper.class,
                LongWritable.class,
                Text.class,
                Text.class,
                IntWritable.class,
                conf);

        // reducer
        ChainReducer.setReducer(job,
                CountDropReducer.class,
                Text.class,
                IntWritable.class,
                Text.class,
                IntWritable.class,
                conf);

        // mapper 2
        ChainReducer.addMapper(job,
                TopDropMapper.class,
                Text.class,
                IntWritable.class,
                Text.class,
                IntWritable.class,
                conf);



        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


        boolean b = job.waitForCompletion(true);
        System.exit(b? 0 : 1);
    }

}

//./hadoop jar /home/zzz/INFO7250/Final/Final/target/Final-1.0-SNAPSHOT.jar TopDropoffLocation.TopDropLocationDriver /final/mergedData.csv /finalOutput/TopDropoff