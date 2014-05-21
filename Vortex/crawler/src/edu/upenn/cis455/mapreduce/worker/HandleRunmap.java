package edu.upenn.cis455.mapreduce.worker;
import edu.upenn.cis455.mapreduce.AllStrings;
public class HandleRunmap extends Thread
{

		public void run()
		{
			try
			{
				  Crawler crawler = new Crawler();				
				  while(Worker.mapStarted && !Worker.shouldStop)
				  {  				
				  	  String line = GetFromRunmapReader.getFromRunmapReader();
				  	  //System.out.println("Am i even coming till here"+line);
					  if(line == null)
					  {
						  GetFromRunmapReader.invokeRunmapReader();
					  }
					  if(line != null)
						  crawler.crawlAndStoreURL(line);
					  if(Worker.dataRead > AllStrings.maxDataToBeRead)
					  {
						   // System.out.println("Max data read limit exceeded and coming out");
					  		Worker.mapStarted = false;
					  }
				  }
			}
			catch (NullPointerException e)
			{
				//TODO log
				//System.out.println("I am null pointer exception");
			} 
		}
	}
