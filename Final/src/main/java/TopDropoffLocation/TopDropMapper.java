package TopDropoffLocation;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class TopDropMapper extends Mapper<Text, IntWritable, Text, IntWritable> {

    // Using treeMap to assist TopN pattern
	private TreeMap<Integer, String> tmap;
	
	@Override
    public void setup(Context context) throws IOException, InterruptedException { 
        tmap = new TreeMap<Integer, String>();
    } 
  
    @Override
    protected void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {

        if(key.toString().equals("0")) return;
//    	String[] line = value.toString().split("\t");
//
//        String location = line[0];
//        int count = Integer.parseInt(line[1]);

        String location = String.valueOf(key);
        int count = value.get();

        tmap.put(count, location);
        
    	if (tmap.size() > 10) {
            tmap.remove(tmap.firstKey()); 
        }
    }
    
	@Override
    public void cleanup(Context context) throws IOException, InterruptedException {
  
        for (Map.Entry<Integer, String> entry : tmap.entrySet()) {
        	int r = entry.getKey();
            String v = entry.getValue(); 
            
            context.write(new Text(v), new IntWritable(r));
        } 
    }	
}

