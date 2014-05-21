package edu.upenn.cis455.imageIndexer;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import edu.upenn.cis455.imageIndexer.IndexMapper;
import edu.upenn.cis455.imageIndexer.IndexReducer;


public class InvertedIndexJob {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();

	     Job job = new Job(conf, "invertedindex");

	     job.setJarByClass(InvertedIndexJob.class);

	     job.setMapperClass(IndexMapper.class);

	     job.setReducerClass(IndexReducer.class);

	     job.setOutputKeyClass(Text.class);

	     job.setOutputValueClass(Text.class);

	     job.setMapOutputKeyClass(Text.class);

	     job.setMapOutputValueClass(Text.class);

	     job.setInputFormatClass(KeyValueTextInputFormat.class);

	     job.setOutputFormatClass(TextOutputFormat.class);

	     FileInputFormat.addInputPath(job, new Path(args[0]));

	     FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    // JobClient.runJob(conf);

	     boolean result = job.waitForCompletion(true);

	     System.exit(result ? 0 : 1);

	    }


}
