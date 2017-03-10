package BD.Hadoop.Task1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class LongestWords extends Configured implements Tool {

    public int run(String[] args) throws Exception {
    	
    	Job job = Job.getInstance(getConf()); // new approach
        
        job.setJobName("LongestWordsJob");
        job.setJarByClass(LongestWords.class); // fix ClassNotFoundException (Map or Reduce class)

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setMapOutputKeyClass(LongWritable.class );
        job.setMapOutputValueClass(Text.class);
        
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        
        job.setSortComparatorClass(LongWritable.DecreasingComparator.class); // Sort descending 
        
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;

    }
        
    public static void main( String[] args ) throws Exception
    {
    	int exitCode = ToolRunner.run(new Configuration(), new LongestWords(), args);
        System.exit(exitCode);
    }
}
