package BD.Hadoop.Task4;

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

public class ImpressionAnalyzerTest 
{
    MapReduceDriver<LongWritable, Text, ImpressionInformation, LongWritable, Text, Text> mapReduceDriver;
    
    @Before
    public void setUp() throws IOException{
    	ImpressionMapper  mapper = new ImpressionMapper();
    	ImpressionReducer reducer = new ImpressionReducer();

    	
    	mapReduceDriver = new MapReduceDriver<LongWritable, Text, ImpressionInformation, LongWritable, Text, Text> ();
    	mapReduceDriver.setMapper(mapper);
    	mapReduceDriver.setReducer(reducer);
    	
    	

    	Configuration conf = mapReduceDriver.getConfiguration();
    	conf.set("impressionanalyzer.city.filepath", "./dictionaries/city.en.txt");

    }
    
    
    
    @Test
    public void testSumBid() throws IOException{
    	// Should not be included 
    	//City: 234, Bid: 277
    	mapReduceDriver.withInput(new LongWritable(), new Text("2e72d1bd7185fb76d69c852c57436d37	20131019025500549	1	CAD06D3WCtf	Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)	113.117.187.*	216	234	2	33235ca84c5fee9254e6512a41b3ad5e	8bbb5a81cc3d680dd0c27cf4886ddeae	null	3061584349	728	90	OtherView	Na	5	7330	277	48	null	2259	10057,13800,13496,10079,10076,10075,10093,10129,10024,10006,10110,13776,10146,10120,10115,10063"));

    	// Should be included
    	//City: 222 (foshan), Bid: 251
    	for (int i=1; i<=251; i++)
    	{
    		mapReduceDriver.withInput(new LongWritable(), new Text("93074d8125fa8945c5a971c2374e55a8	20131019161502142	1	CAH9FYCtgQf	Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; .NET CLR 2.0.50727)	119.145.140.*	216	222	1	20fc675468712705dbf5d3eda94126da	9c1ecbb8a301d89a8d85436ebf393f7f	null	mm_10982364_973726_8930541	300	250	FourthView	Na	0	7323	1	201	null	2259	10057,10059,10083,10102,10024,10006,10110,10031,10063,10116"));
    	}
    	    	
    	List <Pair <Text, Text>> outputRecords = new ArrayList<Pair<Text, Text>>();
       	outputRecords.add(new Pair <Text, Text> (new Text("foshan"), new Text( "251" )));
    	
    	mapReduceDriver.withAllOutput(outputRecords);
    	
    	mapReduceDriver.runTest();
    	
    }
    
}
