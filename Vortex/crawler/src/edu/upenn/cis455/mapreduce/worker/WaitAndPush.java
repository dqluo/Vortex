package edu.upenn.cis455.mapreduce.worker;

import edu.upenn.cis455.mapreduce.AllStrings;


public class WaitAndPush extends Thread
{
	public void run()
	{
		while(true)
		{
			if(Worker.mapStarted)
			{
				if(System.currentTimeMillis()-Worker.recentTimeOfMapIssued > 40000 )
				{	
					Worker.mapStarted = false;
					//System.out.println("Pushed");
					//Thread.currentThread().stop();
					break;
				}	
				synchronized(Worker.pushedData)
				{
					if(Worker.pushedData.length() > AllStrings.maxDataToBeRead)
					{
						Worker.finalSpoolIn();
					}
				}
			}
		}
		//System.out.println("I should come out of it now");
	}
}
