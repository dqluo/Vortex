package edu.upenn.cis455.mapreduce.worker;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

import edu.upenn.cis455.mapreduce.AllStrings;

public class WorkerStatus {

	public static void reportWorkerStatus(String masterhost,int masterport,
			String workerport,String status,int crawlcount)//int keysRead,int keysWritten)
	{
	    Socket soc;
		try 
		{
			soc = new Socket(masterhost,masterport);	
			DataOutputStream bw = new DataOutputStream(soc.getOutputStream()); 
			//make querystring
			StringBuffer queryString = new StringBuffer();
			queryString.append("?");
			queryString.append(AllStrings.portString);
			queryString.append("=");
			queryString.append(workerport);
			queryString.append("&");
			queryString.append(AllStrings.statusString);
			queryString.append("=");
			queryString.append(status);
			queryString.append("&");
			queryString.append(AllStrings.crawlcountString);
			queryString.append("=");
			queryString.append(Crawler.globalCount);
			queryString.append("&");
			queryString.append(AllStrings.seedToStartWithString);
			queryString.append("=");
			if(GetFromRunmapReader.seedcount == 1)
				queryString.append(GetFromRunmapReader.seedcount);
			else
			{ 
				queryString.append((GetFromRunmapReader.seedcount-1));
			}
			queryString.append("&");
			queryString.append(AllStrings.creationCountString);
			queryString.append("=");
			queryString.append(GetFromRunmapReader.creationCount);
			queryString.append("&");
			queryString.append(AllStrings.lineReadTillInBufferedReaderString);
			queryString.append("=");
			queryString.append((GetFromRunmapReader.lineCount));
			//System.out.println(queryString.toString());
			//Make heartbeat
			StringBuffer heartbeat = new StringBuffer();
			heartbeat.append(AllStrings.httpMethodGET);
			heartbeat.append("/workerstatus");
			heartbeat.append(queryString);
			heartbeat.append(" HTTP/1.1\r\n");
			heartbeat.append(AllStrings.host);
			heartbeat.append(masterhost);
			heartbeat.append("\r\n");
			heartbeat.append(AllStrings.connectionClosed);
			heartbeat.append("\r\n\r\n");
			bw.writeBytes(heartbeat.toString());
			bw.flush();
			soc.close();
			//System.out.println(queryString.toString());
		}
		catch (IOException e) 
		{
			e.printStackTrace();
		}
	}
	
}
