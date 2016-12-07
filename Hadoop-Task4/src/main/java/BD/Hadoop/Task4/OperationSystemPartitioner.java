package BD.Hadoop.Task4;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;



public class OperationSystemPartitioner  extends Partitioner<ImpressionInformation,LongWritable> {
 
 @Override
 public int getPartition(ImpressionInformation key, LongWritable value, int numPartitions) {
	 switch ( key.getOperationSystem())
	 {
	 	case ("Windows") :  return 1;    
	 	case ("Linux") : return 2; 
	 	default: return 0;
	 }
	 
  }
}
