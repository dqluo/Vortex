package edu.upenn.cis455.mapreduce.worker;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;


public class GetFromRunmapReader {

	public static BufferedReader seedFilerReaderLoad;
	public static int seedcount = 1;
	public static int creationCount = 2;
	public static int lineCount = 0;
	public static void invokeRunmapReader()
	{
		File seedFileLoad = new File(Worker.seedPath+"/seed"+(seedcount)+".txt");
		System.out.println("From seed reader "+seedFileLoad.getAbsolutePath());
		try  
		{
			System.out.println("I am the current seed count"+seedcount);
			if(seedFileLoad.exists())
			{
				lineCount = 0;
				seedcount++;
				//Load the file in chunks of 20 KB
				seedFilerReaderLoad = new BufferedReader(new FileReader(seedFileLoad),1024*1024);
			}
			else
			{
				while(Worker.mapStarted && !Worker.shouldStop)
				{
					if(seedFileLoad.exists())
					{
						lineCount = 0;
						seedFilerReaderLoad = new BufferedReader(new FileReader(seedFileLoad));
						seedcount++;
						break;
					}
				}
			}
		} 
		catch (FileNotFoundException e)
		{
			//System.out.println("Exception during loading buffered reader");
		} 
	}
	public static String getFromRunmapReader()
	{
		try
		{
			//This code returns a single url from the queue to the requesting thread
			String line = null;
			synchronized(seedFilerReaderLoad)
			{ 
				if((line = seedFilerReaderLoad.readLine())!=null) 
				{
					//System.out.println("I am the line to be returned from get run map reader "+line);
					lineCount++;
					return line;
				}
				else
				{
					
					String line1;
					GetFromRunmapReader.invokeRunmapReader();
					if((line1 = seedFilerReaderLoad.readLine())!=null)
					{
						
						lineCount++;
						return line1;
					}
					
				}
			}
		}
		catch(Exception e)
		{
			//System.out.println("Error while returning a line from the queue");
		}
		return null;
	}	
	
	
	
	

}
