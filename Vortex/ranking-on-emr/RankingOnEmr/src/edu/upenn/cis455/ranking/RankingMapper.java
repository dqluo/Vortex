package edu.upenn.cis455.ranking;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RankingMapper extends Mapper<Text,Text,Text,Text> {


	/*
	 * (Key, value) =
	 * page:rank	child1:0;child2:1;.....
	 * 
	 */
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException
	{
		String values = value.toString();
		String keys = key.toString();

		/* keys = page:rank */
		String pageAndRank[] = keys.split(":");

		String parent  = pageAndRank[0];
		float parentsRank = (Float.parseFloat(pageAndRank[1]));

		/* 
		 * If the key-value is like Page:Rank	-nochildren-
		 */
		
		if(!values.contains(";")) {
			Text parentText = new Text(parent);
			Text parentRankText = new Text(Float.toString(parentsRank));
			context.write(parentText, parentRankText);
			return;
		}

		float childRank = (float)0.0;

		String children[] = values.split(";");		
		int numOfChildren = 0;

		for(String child: children) {
			if(child.contains(":")) {
				/* Number of children is only equal to the number of non dangling links */
				String danglingLinkVariable[] = child.split(":");
				if(danglingLinkVariable[1].equals("1"))
					numOfChildren++;
			}
		}

		if(numOfChildren == 0)
			childRank = (float)0.15;

		else
			/* Computing the rank a child has to get */
			childRank = (float)parentsRank/(float)numOfChildren;

		Text childText = null;
		Text valueText = null;


		/* 
		 * Transmitting the ranks of each child 
		 * child	rank
		 */
		for(String child: children) {
			childText = new Text(child.split(":")[0]);
			valueText = new Text(Float.toString(childRank));
			context.write(childText, valueText);
		}

		Text outputKey = new Text(parent);

		/* 
		 * Writing the out links for the parent URL 
		 * parent	children
		 * 
		 * This is required to distribute the ranks for iterations
		 */
		context.write(outputKey, value);
	}

}
