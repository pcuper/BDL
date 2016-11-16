package BD.Hadoop.Task3;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class LogMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
	// Working RegExp without escape symbols: (?<ip>[ip[0-9]+) - - \[(?<datetime>.+)\] "(.+)" (?<responsecode>[0-9]+)[\s]?(?<contentlength>[-0-9]*) "(.*)" "(?<agent>[\D]+)\/?([0-9]+)[.]?[0-9]* ([(].+[)])"$
    private String LogEntryPattern = new String("(?<ip>\\[ip\\[0-9\\]+) - - \\[(?<datetime>.+)\\] \"(.+)\" (?<responsecode>\\[0-9\\]+)\\[\\s\\]?(?<contentlength>\\[-0-9\\]*) \"(.*)\" \"(?<agent>\\[\\D\\]+)\\/?(\\[0-9\\]+)\\[.\\]?\\[0-9\\]* (\\[(\\].+\\[)\\])\"");
    Pattern r = Pattern.compile(LogEntryPattern);
    
    private Text word = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	
    	String line = value.toString();
    	// Counting lines to check agent counters and regexp-matchers
    	context.getCounter(UserAgent.ALL).increment(1);
    	
        Matcher m = r.matcher(line);
        if (m.find()) {
        	Text ip = new Text( m.group("ip") );
        	//LongWritable responsecode = new LongWritable(Integer.parseInt(m.group("responsecode")));
        	LongWritable contentlength = new LongWritable(Integer.parseInt(m.group("contentlength")));
        	context.write(ip, contentlength);
        	
        	
        	//Counting agents
        	String agent = m.group("agent");
        	agent = agent.toLowerCase();
        	switch (agent)
        	{
        		case "mozilla": 
        			context.getCounter(UserAgent.Mozilla).increment(1); break;
        		case "IE": context.getCounter(UserAgent.IE).increment(1); break;
        		default: context.getCounter(UserAgent.Other).increment(1); 
        	}
        } 
        

    }
}
