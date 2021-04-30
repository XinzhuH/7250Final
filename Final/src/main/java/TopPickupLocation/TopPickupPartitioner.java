package TopPickupLocation;


import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class TopPickupPartitioner extends Partitioner<CompositeKey, DoubleWritable> {

	@Override
    public int getPartition(CompositeKey compositeKey, DoubleWritable doubleWritable, int numPartitions) {
        return compositeKey.getTipPercent().hashCode() % numPartitions;
    }

	

}
