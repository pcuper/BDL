package BD.Hadoop.Task4;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.mortbay.log.Log;


public class ImpressionMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
	final int COLUMN_CITY = 7;  
	final int COLUMN_BIDDING_PRICE = 19;
	
	
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	
    	String line = value.toString();
    	String[] columnValues =  line.split("\t");
    	
    	// Using data table (http://contest.ipinyou.com/data.shtml) to detect column meaning
    	// See Impression description
    	Text city = new Text ( columnValues[COLUMN_CITY] );
    	LongWritable bidding_price = new LongWritable(  Long.parseLong( columnValues[COLUMN_BIDDING_PRICE]));
    	context.write( city, bidding_price );
    }
}
