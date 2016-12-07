package BD.Hadoop.Task4;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.mortbay.log.Log;

import net.sf.uadetector.OperatingSystem;
import net.sf.uadetector.ReadableUserAgent;
import net.sf.uadetector.UserAgentStringParser;
import net.sf.uadetector.service.UADetectorServiceFactory;


public class ImpressionMapper extends Mapper<LongWritable, Text, ImpressionInformation, LongWritable> {	
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	
    	String line = value.toString();
    	String[] columnValues =  line.split("\t");
    	
    	//Preparing agent parser
    	UserAgentStringParser parser = UADetectorServiceFactory.getResourceModuleParser();
		ReadableUserAgent agent = parser.parse( columnValues[ImpressionConsts.Column_useragent] );

    	// Using data table (http://contest.ipinyou.com/data.shtml) to detect column meaning
		// See Impression description
    	ImpressionInformation impressionInformation = 
    			new ImpressionInformation( columnValues[ImpressionConsts.Column_city], agent.getOperatingSystem().getFamilyName());
    	LongWritable bid = new LongWritable( Long.parseLong(columnValues[ImpressionConsts.Column_bidding_price])  );

    	context.write( impressionInformation, bid );
    }
}
