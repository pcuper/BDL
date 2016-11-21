package BD.Hadoop.Task3;

import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;


// To run locally 
//job.getConfiguration().set("mapreduce.framework.name", "local"); 
//job.getConfiguration().set("fs.default.name", "file:///");	


public class LogAnalyzer 
{
    public static void main( String[] args ) throws Exception {
    	if (args.length != 2) {
            System.err.println("Usage: LogAnalyzer <input path> <output path>");
            System.exit(-1);
        }
        Job job = Job.getInstance(); 
        

        job.setJarByClass(LogAnalyzer.class);
        job.setJobName("Log Analyzer");
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath( job, new Path(args[1]));
        
        job.setMapperClass(LogMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        
        //http://blog.optimal.io/3-differences-between-a-mapreduce-combiner-and-reducer/
        //One constraint that a Combiner will have, unlike a Reducer, is that the input/output key and value types must match the output types of your Mapper.
        //job.setCombinerClass(LogReducer.class);
        
        job.setReducerClass(LogReducer.class);
        

//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.getConfiguration().set("mapred.textoutputformat.separator", ",");
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
