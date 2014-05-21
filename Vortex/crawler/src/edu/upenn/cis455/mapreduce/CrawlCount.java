package edu.upenn.cis455.mapreduce;

import edu.upenn.cis455.mapreduce.Context;
import edu.upenn.cis455.mapreduce.Job;

public class CrawlCount implements Job 
{

  public void map(String key, String value, Context context)
  {
	  context.write(key,"");

  }
  
  public void reduce(String key, String[] values, Context context)
  {

    // Your reduce function for WordCount goes here
  }
  
}
