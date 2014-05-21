package edu.upenn.cis455.mapreduce.master;



import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import javax.servlet.*;
import javax.servlet.http.*;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;


public class LogCrawlCountServlet extends HttpServlet
{
  static Logger logger = Logger.getLogger(LogCrawlCountServlet.class);
  static final long serialVersionUID = 455555001;
  


public void init() throws ServletException
{
		PropertyConfigurator.configure("Log/log4j.properties");
	    Thread one  =new Thread() 
		{
		    public void run() 
			{
		    	while(true)
		    	{	
		    		try 
					{
		    			Thread.sleep(120000);
		    		} catch (InterruptedException e) 
					{
		    			e.printStackTrace();
		    		}
		    		if(Master.isMapping)
		    		{
		    		Master.crawlCount = 0;
		    		Iterator<Entry<Integer, WorkerInMaster>> it = Master.workerStatus.entrySet().iterator();
		    	    while (it.hasNext()) 
		    	    {
		    	    	Map.Entry<Integer,WorkerInMaster> pairs = (Map.Entry<Integer,WorkerInMaster>)it.next();
		    	    	if(pairs.getValue().getIsActive() == true)
		    	    	{
		    	    		Master.crawlCount  += pairs.getValue().getKeysWritten();
		    	    	}

		    	    }

		    			String totalTimeElaspsed = MasterServlet.getTimeDiff(System.currentTimeMillis(),Master.timeOfMapIssued);
		    			logger.info("Time elapsed since start  "+totalTimeElaspsed+"  ");
						logger.info("Total crawl count      "+Master.crawlCount);
		    				
		    		}
		    		

		    	}
		    }

				};one.start();
	  }
	  
}
