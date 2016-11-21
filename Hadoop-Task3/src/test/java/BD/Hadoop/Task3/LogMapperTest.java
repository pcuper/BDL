package BD.Hadoop.Task3;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.Console;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapreduce.Reducer;
/* import for setMapper and setReducer*/ 
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
//import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;


import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.hadoop.mrunit.types.*;

import org.apache.hadoop.fs.*;

public class LogMapperTest 
{

    MapDriver <LongWritable, Text, Text, LongWritable > mapDriver;
    MapReduceDriver<LongWritable, Text, Text, LongWritable, Text, LongWritable> mapReduceDriver;
    
    @Before
    public void setUp() throws IOException{
    	LogMapper mapper = new LogMapper();
    	LogReducer reducer = new LogReducer();
    	    	
    	mapDriver = new  MapDriver <LongWritable, Text, Text, LongWritable>();
    	mapDriver.setMapper(mapper);
    	    	
    	mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, LongWritable, Text, LongWritable> ();
    	
    	mapReduceDriver.setMapper(mapper);
    	mapReduceDriver.setReducer(reducer);

    }
    
    @Test
    public void testRegExpMapperAndCounters() throws IOException{
    	mapDriver.withInput(new LongWritable(), new Text("ip1 - - [24/Apr/2011:04:06:01 -0400] \"GET /~strabal/grease/photo9/927-3.jpg HTTP/1.1\" 200 40028 \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\""));
    	// No browser version
    	mapDriver.withInput(new LongWritable(), new Text("ip5 - - [24/Apr/2011:04:25:20 -0400] \"GET / HTTP/1.1\" 200 12550 \"-\" \"Baiduspider+(+http://www.baidu.com/search/spider.htm)\""));
    	//Complex browser string 
    	mapDriver.withInput(new LongWritable(), new Text("ip1568 - - [27/Apr/2011:22:38:01 -0400] \"GET /vanagon/VanagonProTraining/86Vanagon/037.jpg HTTP/1.1\" 200 80518 \"http://www.thesamba.com/vw/forum/viewtopic.php?t=390833\" \"Mozilla/5.0 (Windows; U; Windows NT 5.1; en-GB; rv:1.9.2.16) Gecko/20110319 Firefox/3.6.16 ( .NET CLR 3.5.30729) InquisitiveMindsAddon\""));
    	//No content length/redirect
    	mapDriver.withInput(new LongWritable(), new Text("ip225 - - [25/Apr/2011:18:37:45 -0400] \"GET / HTTP/1.0\" 304 - \"-\" \"Mozilla/5.0 (compatible; Yahoo! Slurp/3.0; http://help.yahoo.com/help/us/ysearch/slurp)\""));
    	//No agent name (only agent comments)
    	mapDriver.withInput(new LongWritable(), new Text("ip990 - - [26/Apr/2011:14:52:51 -0400] \"GET /faq/ HTTP/1.0\" 200 11077 \"http://host2/faq/\" \"(TalkTalk Virus Alerts Scanning Engine)\""));
    	//No agent name (at all)    	
    	mapDriver.withInput(new LongWritable(), new Text("ip26 - - [26/Apr/2011:16:29:14 -0400] \"HEAD /~strabal/TFE.mp3 HTTP/1.1\" 200 0 \"-\" \"-\""));
    	
    	
    	mapDriver.withOutput(new Text("ip1"), new LongWritable(40028));
    	mapDriver.withOutput(new Text("ip5"), new LongWritable(12550));
    	mapDriver.withOutput(new Text("ip1568"), new LongWritable(80518));
    	mapDriver.withOutput(new Text("ip225"), new LongWritable(0));
    	mapDriver.withOutput(new Text("ip990"), new LongWritable(11077));
    	mapDriver.withOutput(new Text("ip26"), new LongWritable(0));
    	
		mapDriver.runTest();
		Assert.assertEquals("Expected 1 counter for Mozilla agent", 3, mapDriver.getCounters().findCounter(UserAgent.Mozilla).getValue());
		Assert.assertEquals("Expected 1 counter for Mozilla agent", 0, mapDriver.getCounters().findCounter(UserAgent.IE).getValue());
		Assert.assertEquals("Expected 1 counter for Other agent", 3, mapDriver.getCounters().findCounter(UserAgent.Other).getValue());
		
		
    }
    
    @Test
    public void testSumContentLength() throws IOException{
    	
    	mapReduceDriver.withInput(new LongWritable(), new Text("ip1 - - [24/Apr/2011:04:06:01 -0400] \"GET /~strabal/grease/photo9/927-3.jpg HTTP/1.1\" 200 40028 \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\""));
    	mapReduceDriver.withInput(new LongWritable(), new Text("ip1 - - [24/Apr/2011:04:18:54 -0400] \"GET /~strabal/grease/photo1/T97-4.jpg HTTP/1.1\" 200 6244 \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\""));
    	mapReduceDriver.withInput(new LongWritable(), new Text("ip5 - - [24/Apr/2011:04:25:20 -0400] \"GET / HTTP/1.1\" 200 12550 \"-\" \"Baiduspider+(+http://www.baidu.com/search/spider.htm)\""));
    
    	List <Pair <Text, LongWritable>> outputRecords = new ArrayList<Pair<Text, LongWritable>>();
    	outputRecords.add(new Pair <Text, LongWritable> (new Text("ip1"), new LongWritable(40028+6244)) );
    	outputRecords.add(new Pair <Text, LongWritable> (new Text("ip5"), new LongWritable(12550)) );
    	
    	mapReduceDriver.withAllOutput(outputRecords);
    	
    	mapReduceDriver.runTest();
    	
    }
    
    /*
    @SuppressWarnings("deprecation")
	@Test
    public void testAllLinesAreProcessed() throws IOException{
    	
    	Configuration conf = mapReduceDriver.getConfiguration();
	    conf.set("fs.defaultFS", "file:///"); 
	    conf.set("mapred.framework.name", "local");
    	
	    FileSystem localfs = FileSystem.getLocal(conf);    	
    	Path path = new Path( localfs.getWorkingDirectory()+"/access_logs/input/000000");
    	
    	InputStreamReader isr =  new InputStreamReader(localfs.open(path));
    	BufferedReader bufferRedaer = new BufferedReader(isr );

        String str = null;
        while ((str = bufferRedaer.readLine())!= null)
        {
           	mapReduceDriver.addInput(new LongWritable(0), new Text(str) );
        }
	    
        mapReduceDriver.withAllOutput(null);
        
    	mapReduceDriver.runTest();

    	long all = mapDriver.getCounters().findCounter(UserAgent.ALL).getValue();
    	long mozilla = mapDriver.getCounters().findCounter(UserAgent.Mozilla).getValue();
    	long ie = mapDriver.getCounters().findCounter(UserAgent.IE).getValue();
    	long other = mapDriver.getCounters().findCounter(UserAgent.Other).getValue();
    	
    	Assert.assertEquals("Sum ( Mozilla + IE + Other counter) = ALL Counter", all , mozilla + ie + other);
    }*/
}
