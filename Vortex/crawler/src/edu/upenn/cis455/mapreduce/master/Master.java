package edu.upenn.cis455.mapreduce.master;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import edu.upenn.cis455.mapreduce.AllStrings;


public class Master 
{
	public static HashMap<Integer,WorkerInMaster> workerStatus = new HashMap<Integer,WorkerInMaster>();
	public static int noOfActiveWorkers;
	public static String ipdirectory;
	public static int noOfMapThreads;
	public static boolean isMapping = false;
	public static boolean shouldpush =true;
	public static  String[] seedToStartWith;
	public static String[] creationcount;
	public static long timeOfMapIssued;
	public static long timeOfStopped;
	public static boolean isStopped = false;
	public static int crawlCount = 0;
	static Logger logger = Logger.getLogger(Master.class);
	public void setFormParameters(String ipdirectory,int noOfMapThreads,String seedToStartWith,String creationcount)
	{
		Master.ipdirectory = ipdirectory;
		Master.noOfMapThreads = noOfMapThreads;
		Master.seedToStartWith = seedToStartWith.split(";");
		Master.creationcount = creationcount.split(";");
	}
	
	
	public static int getWorkerNumber(int port)
	{
		switch(port)
		{
			case 8011: return 1;
			case 8012: return 2;
			case 8013: return 3;
			case 8014: return 4;
			case 8015: return 5;
			case 8016: return 6;
			case 8017: return 7;
			case 8018: return 8;
			case 8019: return 9;
			case 8020: return 10;
		}
		return -1;
	}
	
	
	//To update status of worker from workerstatus
	public static void updateWorkerStatus(Integer workerPort,String workerhost,String status,int keysWritten,int seedcount,int creationcount,int lineCountOfBFR)
	{
		if(workerStatus.containsKey(workerPort))
		{
			WorkerInMaster w = workerStatus.get(workerPort);
			w.setWorkerHost(workerhost);
			w.setStatus(status);
			//w.setKeysRead(keysRead);
			w.setKeysWritten(keysWritten);
			w.setLastEdited();
			w.setIsActive(true);
			w.seedcount = seedcount;
			w.creationcount = creationcount;
			w.lineCountOfBFR = lineCountOfBFR;
			workerStatus.put(workerPort, w);
		}
		else
		{
			WorkerInMaster w = new WorkerInMaster();
			w.setWorkerHost(workerhost);
			w.setStatus(status);
			//w.setKeysRead(keysRead);
			w.setKeysWritten(keysWritten);
			w.setLastEdited();
			w.setIsActive(true);
			w.seedcount = seedcount;
			w.creationcount = creationcount;
			w.lineCountOfBFR = lineCountOfBFR;
			workerStatus.put(workerPort, w);
		}
	}
	//Function to correct or set workers status
	public static void setActiveWorkers()
	{
		Iterator<Entry<Integer, WorkerInMaster>> it = Master.workerStatus.entrySet().iterator();
		 Master.noOfActiveWorkers =0;
		while (it.hasNext()) 
		{
			Map.Entry<Integer,WorkerInMaster> pairs = (Map.Entry<Integer,WorkerInMaster>)it.next();
		   
		    WorkerInMaster w = pairs.getValue();
		    	//w.setIsActive(false);	
		    if(w.getLastEdited() <= System.currentTimeMillis()-30000)
		    {
		    		w.setIsActive(false);
		    }
		    else
		    {
		    	
		    	Master.noOfActiveWorkers++;
		    	w.setIsActive(true);
		    }
		  }
		}
		//Construct worker IP:Port string to send with runmap
	public static String constructIPPortString()
	{
		String result = "";
		Iterator<Entry<Integer, WorkerInMaster>> it = Master.workerStatus.entrySet().iterator();
		int i = 1;
		while (it.hasNext()) 
	    {
	    	Map.Entry<Integer,WorkerInMaster> pairs = (Map.Entry<Integer,WorkerInMaster>)it.next();
	    	WorkerInMaster w = pairs.getValue();
	    	if(pairs.getValue().getIsActive() == true)
	    	{
	    		
	    		String worker = "worker"+getWorkerNumber(pairs.getKey())+"=";
	    		if(!result.isEmpty())
	    		{
	    			result = result+"&"+worker+w.getWorkerHost()+":"+pairs.getKey();
	    		}
	    		else
	    		{
	    			result = worker+w.getWorkerHost()+":"+pairs.getKey();
	    		}
	    		i++;
	    	}
	    }
		
		
		return result;
	}
		//Call runmap on worker with required parameters
		public static void callRunmapFirstTime()
		{
			PropertyConfigurator.configure("Log/log4j.properties");
			logger.info("First time map issued");
			setActiveWorkers();
			Iterator<Entry<Integer, WorkerInMaster>> it = Master.workerStatus.entrySet().iterator();

			
		    while (it.hasNext()) 
		    {
		      Map.Entry<Integer,WorkerInMaster> pairs = (Map.Entry<Integer,WorkerInMaster>)it.next();
		      WorkerInMaster w = pairs.getValue();
		      if(w.getIsActive())
		      {
				  Socket soc;
					try
					{
						soc = new Socket(w.getWorkerHost(),pairs.getKey());
						String postBody;
						postBody = AllStrings.inputDirectoryString+"="+Master.ipdirectory+"&"+
						AllStrings.numOfWorkersString+"="+Master.noOfActiveWorkers+"&"+
						AllStrings.numOfThreadsString+"="+Master.noOfMapThreads+"&"+
						AllStrings.seedToStartWithString +"="+Master.seedToStartWith[getWorkerNumber(pairs.getKey())-1]+"&"+
						AllStrings.creationCountString+"="+Master.creationcount[getWorkerNumber(pairs.getKey())-1];
						postBody = postBody +"&"+ constructIPPortString();
					    DataOutputStream bw = new DataOutputStream(soc.getOutputStream()); 
						bw.writeBytes(AllStrings.httpMethodPOST+"/runmap"+" HTTP/1.1\r\n");
						bw.writeBytes(AllStrings.host+w.getWorkerHost()+"\r\n");
						bw.writeBytes(AllStrings.contentLengthString+postBody.length()+"\r\n");
						bw.writeBytes(AllStrings.contentTypeString+AllStrings.contentTypeValue+"\r\n");
						bw.writeBytes(AllStrings.connectionClosed+"\r\n\r\n");
						bw.writeBytes(postBody+"\r\n\r\n");
						bw.flush();
					    soc.close();
					}
					
					catch (IOException e) 
					{
						logger.error("Error while calling runmap for the first time.");
					}
		      }
		    }
		}
		public static void issueStop()
		{
			setActiveWorkers();
			Iterator<Entry<Integer, WorkerInMaster>> it = Master.workerStatus.entrySet().iterator();
		    while (it.hasNext()) 
		    {
		      Map.Entry<Integer,WorkerInMaster> pairs = (Map.Entry<Integer,WorkerInMaster>)it.next();
		      WorkerInMaster w = pairs.getValue();
		      if(w.getIsActive())
		      {
				  Socket soc;
					try
					{
						soc = new Socket(w.getWorkerHost(),pairs.getKey());
					    DataOutputStream bw = new DataOutputStream(soc.getOutputStream()); 
						bw.writeBytes(AllStrings.httpMethodGET+"/stop"+" HTTP/1.1\r\n");
						bw.writeBytes(AllStrings.host+w.getWorkerHost()+"\r\n");
						bw.writeBytes(AllStrings.contentTypeString+"text/html"+"\r\n");
						bw.writeBytes(AllStrings.connectionClosed+"\r\n\r\n");			
						bw.flush();
					    soc.close();
					}
					catch (IOException e) 
					{
						//e.printStackTrace();
					}
		        }
		      }
		    }
	public static void callRunmap(String workerhost,int port,int seedcount,int creationcount) 
	{
	  //System.out.println("Regular map issued");
	  Socket soc = null;
		try
		{
			soc = new Socket(workerhost,port);
			String postBody;
			postBody = AllStrings.inputDirectoryString+"="+Master.ipdirectory+"&"+
			AllStrings.numOfWorkersString+"="+Master.noOfActiveWorkers+"&"+
			AllStrings.numOfThreadsString+"="+Master.noOfMapThreads+"&"+
			AllStrings.seedToStartWithString +"="+seedcount+"&"+
			AllStrings.creationCountString+"="+creationcount;
			postBody = postBody +"&"+ constructIPPortString();
		    DataOutputStream bw = new DataOutputStream(soc.getOutputStream()); 
			bw.writeBytes(AllStrings.httpMethodPOST+"/runmap"+" HTTP/1.1\r\n");
			bw.writeBytes(AllStrings.host+workerhost+"\r\n");
			bw.writeBytes(AllStrings.contentLengthString+postBody.length()+"\r\n");
			bw.writeBytes(AllStrings.contentTypeString+AllStrings.contentTypeValue+"\r\n");
			bw.writeBytes(AllStrings.connectionClosed+"\r\n\r\n");
			bw.writeBytes(postBody+"\r\n\r\n");
			bw.flush();
		    soc.close();
		}						
		catch (IOException e) 
		{
			//e.printStackTrace();
	
		}
		finally
		{
			try 
			{
				soc.close();
			} catch (IOException e) 
			{
				e.printStackTrace();
			}
		}
	}		    
	
	//UploadForIndexing
	public static void callUploadForIndex(String postBody)
	{
		PropertyConfigurator.configure("Log/log4j.properties");
		setActiveWorkers();
		Iterator<Entry<Integer, WorkerInMaster>> it = Master.workerStatus.entrySet().iterator();

		
	    while (it.hasNext()) 
	    {
	    	String postBodyToBeSent;
	      Map.Entry<Integer,WorkerInMaster> pairs = (Map.Entry<Integer,WorkerInMaster>)it.next();
	      WorkerInMaster w = pairs.getValue();
	      if(w.getIsActive())
	      {
			  Socket soc;
				try
				{
					postBodyToBeSent = postBody+"&"+
				            AllStrings.myWorkerIDString+"="+Master.getWorkerNumber(pairs.getKey());
					soc = new Socket(w.getWorkerHost(),pairs.getKey());
				    DataOutputStream bw = new DataOutputStream(soc.getOutputStream()); 
					bw.writeBytes(AllStrings.httpMethodPOST+"/uploadforindex"+" HTTP/1.1\r\n");
					bw.writeBytes(AllStrings.host+w.getWorkerHost()+"\r\n");
					bw.writeBytes(AllStrings.contentLengthString+postBodyToBeSent.length()+"\r\n");
					bw.writeBytes(AllStrings.contentTypeString+AllStrings.contentTypeValue+"\r\n");
					bw.writeBytes(AllStrings.connectionClosed+"\r\n\r\n");
					bw.writeBytes(postBodyToBeSent+"\r\n\r\n");
					bw.flush();
				    soc.close();
				}
				
				catch (IOException e) 
				{
					logger.error("Error while calling runmap for the first time.");
				}
	      }
	    }
	}
	
	
	public static void callUploadForRank(String postBody)
	{
		PropertyConfigurator.configure("Log/log4j.properties");
		setActiveWorkers();
		Iterator<Entry<Integer, WorkerInMaster>> it = Master.workerStatus.entrySet().iterator();

		
	    while (it.hasNext()) 
	    {
	      Map.Entry<Integer,WorkerInMaster> pairs = (Map.Entry<Integer,WorkerInMaster>)it.next();
	      WorkerInMaster w = pairs.getValue();
	      if(w.getIsActive())
	      {
	    	  String postBodyToBeSent;
			  Socket soc;
				try
				{
					//System.out.println(po)
					postBodyToBeSent = postBody+"&"+
				            AllStrings.myWorkerIDString+"="+Master.getWorkerNumber(pairs.getKey());
					System.out.println(postBodyToBeSent);
					soc = new Socket(w.getWorkerHost(),pairs.getKey());
				    DataOutputStream bw = new DataOutputStream(soc.getOutputStream()); 
					bw.writeBytes(AllStrings.httpMethodPOST+"/uploadforrank"+" HTTP/1.1\r\n");
					bw.writeBytes(AllStrings.host+w.getWorkerHost()+"\r\n");
					bw.writeBytes(AllStrings.contentLengthString+postBodyToBeSent.length()+"\r\n");
					bw.writeBytes(AllStrings.contentTypeString+AllStrings.contentTypeValue+"\r\n");
					bw.writeBytes(AllStrings.connectionClosed+"\r\n\r\n");
					bw.writeBytes(postBodyToBeSent+"\r\n\r\n");
					bw.flush();
				    soc.close();
				}
				
				catch (IOException e) 
				{
					logger.error("Error while calling runmap for the first time.");
				}
	      }
	    }
	}
	
	
	
	
	
	
	
	
}
		

