package BD.Hadoop.Task3;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class LogReducer extends Reducer<Text, LongWritable, Text, Text> {
	
    public void reduce(Text key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        int avg = 0;
        int num = 0;
        for (LongWritable val : values) {
            sum += val.get();
            num++;
        }
        avg = sum/(num);        
        
        String output = Integer.toString(avg) + "," + Integer.toString(sum);
        
        context.write(key, new Text(output));
    }
    
}

