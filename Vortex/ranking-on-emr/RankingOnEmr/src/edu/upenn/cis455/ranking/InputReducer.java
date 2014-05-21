package edu.upenn.cis455.ranking;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InputReducer extends Reducer<Text, Text, Text, Text> {

	/*
	 * 
	 * Output of reduce: 
	 * 
	 * parent:rank	child:0; child:1; ....
	 * 
	 */
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		String children = "";
		
		for(Text child: values) {
			children += child+";";
		}
		
		Text parentWithRankText = new Text(key+":1.0");
		Text childrenText = new Text(children);
		
		context.write(parentWithRankText, childrenText);
	}
}
