package BD.Hadoop.Task4;

import java.io.BufferedReader;
import java.io.IOException;
//import java.nio.file.Path;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.sun.tools.javac.util.List;

import net.sf.uadetector.UserAgent;
import net.sf.uadetector.UserAgentStringParser;

public class ImpressionReducer extends Reducer<ImpressionInformation, LongWritable, Text, Text> {
	
	Hashtable cityMap = new Hashtable();  
		
	@Override
	protected void setup(Reducer<ImpressionInformation, LongWritable, Text, Text>.Context context)
			throws IOException, InterruptedException {

		// Prepare city map
		Hashtable cityMap = new Hashtable ();
		
		String cityFilePath = context.getConfiguration().get("impressionanalyzer.city.filepath");
		Path path = new Path(cityFilePath);
	    FileSystem fs = path.getFileSystem(context.getConfiguration());
	    FSDataInputStream inputStream = fs.open(path);
	    BufferedReader br=new BufferedReader(new InputStreamReader(inputStream));
        String line;
        line=br.readLine();
        while (line != null){
    		String [] values = line.split("[ \t]");
    		cityMap.put(values[0] , values[1] );
            line=br.readLine();
        }
		super.setup(context);
	}
	
    public void reduce(ImpressionInformation key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {
        
    	long num = 0;
    	long sum = 0;
    	
    	Iterator<LongWritable> iter = values.iterator();
    	while (iter.hasNext()) {
    		num++;
			sum += iter.next().get();
			    		
    		}
    	
    	
    	
		//for (LongWritable val : values) {
//    		num++;
			//sum += val.get();
			
        //}
		
		if (num > 250)
		{
			Text cityName = new Text( "");
			if (cityMap.get(key.toString()) != null)
			{
				cityName =  new Text( cityMap.get(key.getCity()).toString() );
			}
			else
			{
				cityName = new Text(key.getCity());
			}
			
			Text sumBid = new Text ( Long.toString(sum) );
	        context.write(cityName, sumBid );
		}
    	
    }
    
}

