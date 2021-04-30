package AvgPassenger;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class AvgPassengerMapper extends Mapper<Object, Text, Text, IntWritable> {

    int passenger;

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

//        if(key.toString().equals("0")) return;

        String[] line = value.toString().split(",");

        try {
            if(!line[4].equals("passenger_count")) {
                // get passenger number (line[4])
                passenger = (int) Float.parseFloat(line[4]);
            }
            else return;

            // set pickup date time format (line[2]), then get datetime
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Calendar c = Calendar.getInstance();
            c.setTime(sdf.parse(line[2]));

            // get day of the week in String, and create key
            SimpleDateFormat sdf2 = new SimpleDateFormat("EE");
            int dayOfWeek = c.get(Calendar.DAY_OF_WEEK);
            String dayKey = sdf2.format(dayOfWeek);;

            // total passengers & trips in different day of week
            context.write(new Text(dayKey), new IntWritable(passenger));


        } catch (ParseException e) {
                e.printStackTrace();
            }

        }


    }
