/**
 * 
 */
package BD.Hadoop.Task4;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import com.google.common.collect.ComparisonChain;

public class ImpressionInformation implements WritableComparable<ImpressionInformation> {

	private String city = "";
	private String operatingSystem = "";
	
	// To avoid java.lang.RuntimeException: java.lang.NoSuchMethodException: ...<init>()
	//http://stackoverflow.com/questions/11446635/no-such-method-exception-hadoop-init
	public ImpressionInformation(){	}
	
	public ImpressionInformation( String inCity, String inOperatingSystem )
	{
		city = inCity;
		operatingSystem = inOperatingSystem;
	}
	
	
	public String getCity() { return this.city; };
	
	public String getOperationSystem() { return this.operatingSystem; };
	
	
		
	@Override
	public void write(DataOutput out) throws IOException {
		//out.writeUTF(city);
		//out.writeUTF(operatingSystem);
		WritableUtils.writeString(out, city);
		WritableUtils.writeString(out, operatingSystem);
		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		//city = in.readUTF();
        //operatingSystem = in.readUTF();
		this.city = WritableUtils.readString(in);
		this.operatingSystem = WritableUtils.readString(in);
	}


	@Override
	public int compareTo(ImpressionInformation info) {
		//Text txt = new Text(city);
		//return txt.compareTo(o.city.getBytes(), 0, o.city.length());
		/*if (info == null) return 0;
		int intcity = city.compareTo(info.city);
		return intcity == 0 ? operatingSystem.compareTo(info.operatingSystem) : intcity;*/
		int result = ComparisonChain.start().compare(city, info.city)
		        .compare(operatingSystem, info.operatingSystem).result(); 
		return result;
	}
	
	@Override
	public boolean equals(Object o) { 
		//if (o == this) return true;
	    if (!(o instanceof ImpressionInformation)) {
	        return false;
	    }
	
	    ImpressionInformation info = (ImpressionInformation) o;
	
	    return info.city.equals(city) &&
	           info.operatingSystem.equals(operatingSystem);
	}
	
	 @Override
	 public int hashCode() {
		 
		 HashCodeBuilder hashCodeBuilder =new HashCodeBuilder(17, 31);
		 hashCodeBuilder = hashCodeBuilder.append(city).append(operatingSystem);
		 return  hashCodeBuilder.toHashCode();
	        
	 }


}
