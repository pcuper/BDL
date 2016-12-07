package BD.Hadoop.Task4;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

//import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.snappy.SnappyCompressor;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
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


public class ImpressionAnalyzer {
  public static void main( String[] args ) throws Exception {
    String commandFormat = "ImpressionAnalyzer <input path> <output path> <city file path>";

    if ((args.length != 3) ) {
      System.err.println("Usage: "+commandFormat);
            System.exit(-1);
        }
        Job job = Job.getInstance();
        
        job.setJarByClass(ImpressionAnalyzer.class);
        job.setJobName("Impression Analyzer");
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath( job, new Path(args[1]));
        
        Path path = new Path(args[2]);
	    FileSystem fs = path.getFileSystem(job.getConfiguration());
	    
        
        boolean cityFileExists = fs.exists(path);
        if (!cityFileExists)
        {
        	throw new Exception("File with city name does not exist"); 
        }
        
        job.getConfiguration().set("impressionanalyzer.city.filepath", args[2]);
        
        job.setMapperClass(ImpressionMapper.class);
        job.setMapOutputKeyClass(ImpressionInformation.class);
        job.setMapOutputValueClass(LongWritable.class);
        
        job.setPartitionerClass(OperationSystemPartitioner.class);
        
        //job.setSortComparatorClass(ImpressionInformation.class);
        
        job.setReducerClass(ImpressionReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
