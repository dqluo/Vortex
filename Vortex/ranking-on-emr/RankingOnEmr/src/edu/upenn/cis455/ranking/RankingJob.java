package edu.upenn.cis455.ranking;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class RankingJob {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		int numOfIterations = 25;

		String rawInput = "s3://iwsranking/RankerInput";
	//	String primaryInput = "s3://iwsranking/RankerInput";
		String primaryOutput = "s3://iwsranking/outputs/primaryOutput"+System.currentTimeMillis();
		String intermediateOutput = "s3://iwsranking/outputs/output"+System.currentTimeMillis()+"-";


		Configuration conf = new Configuration();
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "ranking");
		job.setJarByClass(RankingJob.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapperClass(ChildToParentMapper.class);
		job.setReducerClass(ChildToParentReducer.class);
		FileInputFormat.addInputPath(job, new Path(rawInput));
		FileOutputFormat.setOutputPath(job, new Path(intermediateOutput+"-2"));
		
		while(!job.waitForCompletion(true)){}
		
		Configuration conf2 = new Configuration();
		@SuppressWarnings("deprecation")
		Job job2 = new Job(conf2, "ranking");
		job2.setJarByClass(RankingJob.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setInputFormatClass(KeyValueTextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		job2.setMapperClass(IdentityMapper.class);
		job2.setReducerClass(InputReducer.class);
		FileInputFormat.addInputPath(job2, new Path(intermediateOutput+"-2"));
		FileOutputFormat.setOutputPath(job2, new Path(intermediateOutput+"-1"));
		
		while(!job2.waitForCompletion(true)){}


		for(int i = 0; i<numOfIterations; i++) {
			
			Configuration conf3 = new Configuration();
			@SuppressWarnings("deprecation")
			Job job3 = new Job(conf3, "ranking");
			job3.setJarByClass(RankingJob.class);
			job3.setOutputKeyClass(Text.class);
			job3.setOutputValueClass(Text.class);
			job3.setMapOutputKeyClass(Text.class);
			job3.setMapOutputValueClass(Text.class);
			job3.setInputFormatClass(KeyValueTextInputFormat.class);
			job3.setOutputFormatClass(TextOutputFormat.class);

			job3.setMapperClass(RankingMapper.class);
			job3.setReducerClass(RankingReducer.class);

			if(i == 0) {
				FileInputFormat.addInputPath(job3, new Path(intermediateOutput+"-1"));
				FileOutputFormat.setOutputPath(job3, new Path(intermediateOutput+i));
				System.out.println("Input path for iteration :"+i+" is "+intermediateOutput+"-1");
				System.out.println("Output path for iteration :"+i+" is "+intermediateOutput+i);
			}
			else if(i == numOfIterations -1){
				int intermediate = i-1;
				FileInputFormat.addInputPath(job3, new Path(intermediateOutput+intermediate));
				FileOutputFormat.setOutputPath(job3, new Path(primaryOutput));
				System.out.println("Input path for iteration :"+i+" is "+intermediateOutput+intermediate);
				System.out.println("Output path for iteration :"+i+" is "+primaryOutput);

			}
			else {
				int intermediate = i-1;
				FileInputFormat.addInputPath(job3, new Path(intermediateOutput+intermediate));
				FileOutputFormat.setOutputPath(job3, new Path(intermediateOutput+i));
				System.out.println("Input path for iteration :"+i+" is "+intermediateOutput+intermediate);
				System.out.println("Output path for iteration :"+i+" is "+intermediateOutput+i);

			}

			while(!job3.waitForCompletion(true)){}

		}

	}
}
