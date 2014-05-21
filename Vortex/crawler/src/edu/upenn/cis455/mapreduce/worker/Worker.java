package edu.upenn.cis455.mapreduce.worker;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;

import edu.upenn.cis455.mapreduce.AllStrings;

public class Worker 
{
	public static boolean isMapFirstTime = true;
	public static String workerIPport;
	public static int workerport;
	public static String status = "none";
	public static int masterport;
	public static String masterhost;
	public static int noOfMapThreads;
	public static String ipdirectory;
	public static String storagedir;
	public static int noOfWorkers;
	public static String[] IPPort ;
	public static int me;
	public static boolean mapStarted = false;
	public static String seedPath;
	//public static String spoolinPath;
	public static String spooloutPath;
	public static String databasestore;
	public static int noOfWorkersPushed = 0;
	public static boolean startOfMap = false;
	//This is the data that has to be pushed at intervals
	public static StringBuffer pushedData = new StringBuffer();
	public static boolean shouldStop = false;
	public static String mySpoolPath;
	public static long recentTimeOfMapIssued;
	public static int dataRead = 0;
	public static void createSpoolOut(String path) throws IOException
	{
		File spoolout = new File(path);
		 spoolout.mkdir(); 
		     
		for(int i=1;i<=Worker.noOfWorkers;i++)
		{
			StringBuffer sb = new StringBuffer();
			sb.append(path);
			sb.append("/worker");
			sb.append(i);
			sb.append("out.txt");
			File file = new File(sb.toString());
			file.createNewFile();
		}
	}
	
	public static void createSpoolIn(String path) throws IOException
	{
		File spoolin = new File(path);
		 spoolin.mkdir(); 
		File file = new File(path+"/seed.txt");
		file.createNewFile();
	}
	public static void createSeed(String path) throws IOException
	{
		File spoolin = new File(path);
		 spoolin.mkdir(); 
		File file = new File(path+"/seed.txt");
		file.createNewFile();
	}
	
	synchronized public static void writeToFile(File file,String content)
	{
		FileWriter fw;
		try
		{
			synchronized(file)
			{
			fw = new FileWriter(file.getAbsoluteFile());

				PrintWriter pw = new PrintWriter(fw);
				pw.write(content);
				pw.close();
				fw.close();
			}
		}
		catch (IOException e) 
		{
			//e.printStackTrace();
		}
	}
	public static void deleteDirectory(File file) throws IOException
	{
		if(file.isDirectory())
		{
			if(file.list().length==0)
			{
				file.delete();
			}
			else
			{
				String files[] = file.list();
				for (String temp : files) 
				{
            	      File fileDelete = new File(file, temp);
            	      deleteDirectory(fileDelete);
            	}
            	if(file.list().length==0)
            	{
               	     file.delete();
            	}
        	}
         }
		 else
		 {
			 file.delete();
         }
     }
	public static void deleteAndCreateSpoolOut(String path)
	{
		File directory = new File(path);
		
		try 
		{
			if(directory.exists())
				deleteDirectory(directory);
				createSpoolOut(path);
		} 
		catch (IOException e) 
		{
			
			//System.out.println("I am I/O exception in create and delete Spool out");
		}
		
	}
	public static void deleteAndCreateSpoolIn(String path)
	{
		File directory = new File(path);
		
		try 
		{
			if(directory.exists())
				deleteDirectory(directory);
				createSpoolIn(path);
		} 
		catch (IOException e) 
		{
			//System.out.println("I am I/O exception in create and delete Spool in");
		}
		
	}
	public static void deleteAndCreateSeedPath(String path)
	{
		File directory = new File(path);
		
		try 
		{
			if(directory.exists())
				deleteDirectory(directory);
				createSpoolIn(path);
		} 
		catch (IOException e) 
		{
			//System.out.println("I am I/O exception in create and delete Spool in");
		}
		
	}
	public static void pushToAllOthers()
	{
		  for(int i= 1;i<=Worker.noOfWorkers;i++)
		  {
			  if(i == Worker.me)
				  continue;
			  String []split = Worker.IPPort[i].split(":");
			  String host = split[0];
			  int port = Integer.valueOf(split[1]);
			  String content = getPushContent(i);
			  //System.out.println("Pushing to  "+i+"   "+content);
			  pushPost(host,port,content);
		  }
	}
	
	public static void pushPost(String host, int port,String postBody)
	{
		Socket soc;
		try
		{
			soc = new Socket(host,port);
		    DataOutputStream bw = new DataOutputStream(soc.getOutputStream()); 
		    StringBuffer sb = new StringBuffer();

			sb.append(AllStrings.httpMethodPOST);
			sb.append("/pushdata");
			sb.append(" HTTP/1.1\r\n");
			sb.append(AllStrings.host);
			sb.append(host);
			sb.append("\r\n");
			sb.append(AllStrings.contentLengthString);
			sb.append(postBody.length());
			sb.append("\r\n");
			sb.append(AllStrings.contentTypeString);
			sb.append(AllStrings.contentTypeValue);
			sb.append("\r\n");
			sb.append(AllStrings.connectionClosed);
			sb.append("\r\n\r\n");
			sb.append(postBody);
			sb.append("\r\n\r\n");
			bw.writeBytes(sb.toString());
			bw.flush();
		    soc.close();
		}
	    catch (IOException e) {
			//System.out.println("Exception in pushing data");
		}
	}
	public static String getPushContent(int i)
	{
		try 
		{
		StringBuffer toBeRead = new StringBuffer();
		toBeRead.append(Worker.spooloutPath);
		toBeRead.append("/worker");
		toBeRead.append(i);
		toBeRead.append("out.txt");
		StringBuffer content = new StringBuffer();
		BufferedReader reader;		
		reader = new BufferedReader(new FileReader(toBeRead.toString()));		
		String line;
		while ((line = reader.readLine()) != null)
		{
			content.append(line);
			content.append("\n");
		}
		if(content == null || content.toString().isEmpty())
		{
			content.append("");
		}
		reader.close();
		return content.toString();
		} 
		catch (FileNotFoundException e) {
			//System.out.println("Exception while getting content to be pushed");
		} catch (IOException e)
		{
			//System.out.println("Exception while getting content to be pushed");
		}
		//If there is nothing to be returned the worker just pushes empty data
		return "";
	}
	
	public static void writeInMySpool()
	{
		try {
		StringBuffer content = new StringBuffer();

		BufferedReader reader;	
		reader = new BufferedReader(new FileReader(Worker.mySpoolPath));
		String line;
		while ((line = reader.readLine()) != null)
		{
			content.append(line);
			content.append("\n");
		}
		if(content == null || content.toString().isEmpty())
		{
			content.append("");
		}
		synchronized(Worker.pushedData)
		{
			Worker.pushedData.append(content);
		}
		reader.close();
		} 
		catch (FileNotFoundException e)
		{	
			//System.out.println("File not found exception while writitn to my own spool in");
		} 
		catch (IOException e) 
		{
			//System.out.println("I/O exception while writitn to my own spool in");
		}
		
	}
	synchronized public static void finalSpoolIn()
	{
		File file = new File(Worker.seedPath+"/seed"+(GetFromRunmapReader.creationCount++)+".txt");
		try
		{
			synchronized(Worker.pushedData)
			{
				//System.out.println("I am creating a new seed of length "+Worker.pushedData.length());
				Worker.writeToFile(file, Worker.pushedData.toString());
				Worker.pushedData.setLength(0);
			}
		}
		catch(Exception e)
		{
			return;
		}

	}


/*	public static void copyFile(File source, File dest) 
	{
		try 
		{
			Files.copy(source.toPath(), dest.toPath());
		} 
		catch (IOException e)
		{
			// TODO log
			System.out.println("Error while copying files");
		}
	}*/
 
	public static void initializeFilePaths()
	{
		  if(Worker.storagedir.endsWith("/"))
		  {
			  Worker.storagedir= Worker.storagedir.substring(0, Worker.storagedir.length()-1);
		  }
		  if(Worker.ipdirectory.startsWith("/"))
		  {
			  Worker.ipdirectory = Worker.ipdirectory.substring(0, Worker.ipdirectory.length()-1);
		  }
		  Worker.ipdirectory = Worker.storagedir +"/"+Worker.ipdirectory;
		  Worker.spooloutPath = Worker.ipdirectory+"/spool-out";
		  Worker.seedPath = Worker.ipdirectory+"/seed";
		  Worker.databasestore = Worker.ipdirectory+"/store";

	}

	public static void deleteAlreadyProcessedSeeds()
	{
		for(int i=1;i<GetFromRunmapReader.seedcount-1;i++)
		{
			File seedFileDel = new File(Worker.seedPath+"/seed"+i+".txt");
			if(seedFileDel.exists())
				seedFileDel.delete();
		}
	}
}
