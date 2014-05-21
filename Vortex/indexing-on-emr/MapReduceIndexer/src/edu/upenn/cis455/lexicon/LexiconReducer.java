package edu.upenn.cis455.lexicon;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LexiconReducer extends Reducer<Text, Text, Text, Text> {
	private Text result = new Text();



	 @Override

	 protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

	     String val1="";

	     for (Text val : values) {
	    	 
	 		val1+=val;

	     }
	     
	    	 result.set(val1);
		     context.write(key, result);
	    	 
	 		
	     
	    
	     

	    }

}
