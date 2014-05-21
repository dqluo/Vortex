package edu.upenn.cis455.mapreduce.worker;


import java.util.ArrayList;
import java.util.Date;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import edu.upenn.cis455.mapreduce.AllStrings;
import edu.upenn.cis455.mapreduce.storage.CrawlerStore;

public class RunmapServlet extends HttpServlet 
{
	static Logger loggerRunmap= Logger.getLogger(RunmapServlet.class);
	

	  public void doPost(HttpServletRequest request, HttpServletResponse response) throws java.io.IOException
	  {
		  PropertyConfigurator.configure("Log/log4j.properties");
		  //TODO do it only once
		  Worker.mapStarted = true;
		  Worker.status= "mapping";
		  Worker.recentTimeOfMapIssued = System.currentTimeMillis();
		  int seedCount = Integer.valueOf(request.getParameter(AllStrings.seedToStartWithString));
		  int creationCount = Integer.valueOf(request.getParameter(AllStrings.creationCountString));
		  Worker.dataRead =0;
		  if(Worker.isMapFirstTime)
		  {
			  Worker.isMapFirstTime = false;
			  GetFromRunmapReader.seedcount = seedCount;
			  GetFromRunmapReader.creationCount = creationCount;
			  Worker.noOfMapThreads = Integer.valueOf(request.getParameter(AllStrings.numOfThreadsString));
			  Worker.ipdirectory = request.getParameter(AllStrings.inputDirectoryString);
			  Worker.noOfWorkers = Integer.valueOf(request.getParameter(AllStrings.numOfWorkersString));
			  Worker.IPPort = new String[Worker.noOfWorkers+1];
			  Worker.pushedData = new StringBuffer();
			  Worker.noOfWorkersPushed = 0;
			  Worker.initializeFilePaths();
			  for(int i= 1;i<=Worker.noOfWorkers;i++)
			  {  
				  String ipport = request.getParameter("worker"+i);
				  if(Integer.valueOf(ipport.split(":")[1]).equals(Worker.workerport))
					  Worker.me = i;
				  Worker.IPPort[i] = ipport;	
				  //Initialize all the paths
				  System.out.println("Workers and port combination  "+i+"  "+Worker.IPPort[i]);
				  System.out.println("Crawl started at   " +Worker.me+"   ");
				  loggerRunmap.info("Crawl started at   " +Worker.me+"   "+ new Date(System.currentTimeMillis()));
			  }

			  GetFromRunmapReader.invokeRunmapReader();
		  }
		  StringBuffer toBeRead  = new StringBuffer();
		  toBeRead.append(Worker.spooloutPath);
		  toBeRead.append("/worker");
		  toBeRead.append(Worker.me);
		  toBeRead.append("out.txt");
		  Worker.mySpoolPath = toBeRead.toString();
		 // Worker.deleteAndCreateSpoolIn(Worker.spoolinPath);
		  Worker.deleteAndCreateSpoolOut(Worker.ipdirectory+"/spo ol-out");
		  //System.out.println("Map started");
		  CrawlerStore cs = new CrawlerStore(Worker.databasestore);
		  cs.openEnv();
		  cs.openDBTables();
		  Crawler.cs = cs;
		  //End of code to delete and create all spool-out local directories
		  ArrayList<Thread> alOfThreads = new ArrayList<Thread>();
		  for(int i=0;i<Worker.noOfMapThreads;i++)
		  {
			  Thread t = new Thread(new HandleRunmap());
			  alOfThreads.add(t);
			  t.start();
		  }
		  boolean allAreDead= false;
		  Thread t1 = new Thread(new WaitAndPush());
		  t1.start();
		  while(!allAreDead)
		  {
			  allAreDead = checkIfAllThreadsAreDead(alOfThreads);
		  }
		  //System.out.println("All are dead"+allAreDead);
		  //Rechecking the death of all threads
		  allAreDead = false;
		  while(!allAreDead)
		  {
			  allAreDead = checkIfAllThreadsAreDead(alOfThreads);
		  }
		  //System.out.println("All are dead"+allAreDead);
		  Worker.pushToAllOthers();
		  if(!Worker.shouldStop)
		  {
			  //System.out.println("Pushing  "+Worker.dataRead);
			 
			  Worker.writeInMySpool();
			  Worker.finalSpoolIn();
			  Worker.deleteAlreadyProcessedSeeds();
			  Worker.status = "waiting";
			  cs.closeDBTables();	
			  WorkerStatus.reportWorkerStatus(Worker.masterhost, Worker.masterport , Worker.workerIPport, Worker.status,Crawler.globalCount);
			    
		  }
		  else
		  {
			  Worker.writeInMySpool();
			  Worker.finalSpoolIn();
			  Worker.deleteAlreadyProcessedSeeds();
			  Worker.status = "stopped";
			  cs.closeDBTables();	
			  WorkerStatus.reportWorkerStatus(Worker.masterhost, Worker.masterport , Worker.workerIPport, Worker.status,Crawler.globalCount);
			    
		  }
	  }
	  public boolean checkIfAllThreadsAreDead(ArrayList<Thread> alOfThreads )
	  {
		  for(int i = 0; i<alOfThreads.size();i++)
		  {
			  if(alOfThreads .get(i).isAlive())
			  {
//				  System.out.println("Wait and push"+Worker.waitAndPushThreadName "Alive  "+alOfThreads.get(i).getName());
				  return false;
			  }
		  }
		  return true;
	  }
	  
}
