package edu.upenn.cis455.ranking;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ChildToParentMapper extends Mapper<Text,Text,Text,Text> {


	/*
	 * (Key, value) =
	 * page	child1;child2;.....
	 * 
	 */
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException
	{
		String values = value.toString();
		String parent = key.toString();

		Text parentText = new Text(parent);

		String children[] = values.split(";");		
		for(String child: children) {
			Text childText = new Text(child);
			context.write(childText, parentText);
		}

		Text parentIndicator = new Text("P");
		context.write(parentText, parentIndicator);
	}

}
