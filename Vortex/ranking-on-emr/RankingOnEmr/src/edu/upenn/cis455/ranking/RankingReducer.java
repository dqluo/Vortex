package edu.upenn.cis455.ranking;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RankingReducer extends Reducer<Text, Text, Text, Text> {

	/*
	 * 
	 * Output of reduce: 
	 * Page:rank	child1:0;child2:1;....
	 * 
	 */
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {


		float value = (float)0.0;

		String children = "";

		for(Text v: values) {
			String valueString = v.toString();

			/* Value contains children */
			if(valueString.contains(";")) {
				children = valueString;
				continue;
			}

			else if(valueString.length()==0)
				continue;

			
			/* Compute the ranks that we receive for the page */
			Float rank = Float.parseFloat(valueString);
			value = value + rank;
		}
		value = (float) (0.15 + 0.85*(value));


		String keyString = key.toString();
		String keyAndRank[] = keyString.split(":");

		Text keyText;
		Text valueText;

		/*if(value==(float)0.15)
			keyText = new Text(key);
		else*/
		keyText = new Text(keyAndRank[0]+":"+value);

		
		/* Emit Page:rank	child:0;child:1.... */
		valueText = new Text(children);
		context.write(keyText, valueText);
	}
}
