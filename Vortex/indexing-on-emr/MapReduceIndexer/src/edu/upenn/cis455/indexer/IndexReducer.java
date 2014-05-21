package edu.upenn.cis455.indexer;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IndexReducer extends Reducer<Text, Text, Text, Text> {
	private Text result = new Text();



	 @Override

	 protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

	     String val1="";
	     int val2=0;

	     for (Text val : values) {
	    	 String value=val.toString().trim();
	    	 String sep="<*LEXSEPARATOR*>";
	 		//String b=value.toString().trim();
	 		int index=value.indexOf(sep); 
	 		String vald=value.substring(0,index);
	 		val2+=Integer.parseInt(value.substring(vald.length()+sep.length()));
	 		val1+="<@@@!REDUCESEPARATOR!@@@>"+vald;

	     }
	     if(val2>0){
	    	 result.set(val1);
		     context.write(key, result);
	    	 
	 		}
	     
	    
	     

	    }

}
