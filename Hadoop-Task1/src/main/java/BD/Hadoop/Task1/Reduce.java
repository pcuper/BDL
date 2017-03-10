package BD.Hadoop.Task1;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.ReduceContext;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce extends Reducer<LongWritable, Text, LongWritable, Text > {

	public void run(Context context) throws IOException, InterruptedException {
		setup(context);
		try {
			//while (context.nextKey()) {
			if (context.nextKey()) {    			
				reduce(context.getCurrentKey(), context.getValues(), context);
				// If a back up store is used, reset it
				Iterator <Text> iter = context.getValues().iterator();
				if(iter instanceof ReduceContext.ValueIterator) {
					((ReduceContext.ValueIterator<Text>)iter).resetBackupStore();        
				}
			}
		} finally {
			cleanup(context);
		}
	}
	
	
    public void reduce(LongWritable length, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
    	Text summaryText = new Text();
    	
        for (Text val : values) {
        	String output = val + ";"; 
        	summaryText.append(output.getBytes(), 0, output.length()); 
        }        	
        context.write(length, summaryText);
    }
}