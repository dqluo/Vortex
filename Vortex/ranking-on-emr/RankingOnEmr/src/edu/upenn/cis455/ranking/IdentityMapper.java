package edu.upenn.cis455.ranking;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class IdentityMapper extends Mapper<Text,Text,Text,Text> {

	public void map(Text key, Text value, Context context) throws IOException, InterruptedException
	{
				context.write(key, value);
	}

}