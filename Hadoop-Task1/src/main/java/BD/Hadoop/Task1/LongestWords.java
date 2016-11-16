package BD.Hadoop.Task1;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.ReduceContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

//import WordCount.Map;
//import WordCount.Reduce;


public class LongestWords extends Configured implements Tool {

    public int run(String[] args) throws Exception {
    	
    	Job job = Job.getInstance(getConf()); // new approach
        
        job.setJobName("LongestWordsJob");
        job.setJarByClass(LongestWords.class); // fix ClassNotFoundException (Map or Reduce class)

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        //NEW1
        job.setMapOutputKeyClass(LongWritable.class );
        job.setMapOutputValueClass(Text.class);
        //--NEW1
        
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        
        
        
        job.setSortComparatorClass(LongWritable.DecreasingComparator.class); // Sort descending 
        
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;

    }
    
        
    public static class Map extends Mapper<LongWritable, Text, LongWritable, Text > {
        private final static LongWritable length = new LongWritable(1);
        private Text word = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
            	word.set(tokenizer.nextToken());                
                length.set(word.getLength());
                context.write(length, word );
            }
        }
    }

    
    public static class Reduce extends Reducer<LongWritable, Text, LongWritable, Text > {

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

    
    
    public static void main( String[] args ) throws Exception
    {
    	int exitCode = ToolRunner.run(new Configuration(), new LongestWords(), args);
        System.exit(exitCode);
    }
}
