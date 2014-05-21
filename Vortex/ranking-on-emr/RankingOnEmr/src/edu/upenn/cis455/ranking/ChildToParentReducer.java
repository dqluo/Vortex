package edu.upenn.cis455.ranking;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ChildToParentReducer extends Reducer<Text, Text, Text, Text> {

	/*
	 * 
	 * Output of reduce: 
	 * 
	 * parent	child:0; child:1; ....
	 * 
	 */
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		String isNotDangling = ":0";
		String child = key.toString();
		
		ArrayList<String> listOfParents = new ArrayList<String>();
		
		for(Text parents: values) {
			String parentString = parents.toString();
			
			if(parentString.equals("P")) {
				isNotDangling = ":1";
				continue;
			}
			listOfParents.add(parentString);
		}

		
		/* 
		 * emit
		 * parent	child:0
		 * or
		 * parent	child:1
		 */
		Text parentText, childText;
		for(String parent: listOfParents) {
			parentText = new Text(parent);
			childText = new Text(child+isNotDangling);
			context.write(parentText, childText);
		}
		
	}
}
