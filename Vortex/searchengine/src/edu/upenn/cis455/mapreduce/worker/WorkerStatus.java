package edu.upenn.cis455.mapreduce.worker;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

import edu.upenn.cis455.mapreduce.AllStrings;

public class WorkerStatus {

	public static void reportWorkerStatus(String masterhost,int masterport,String workerport,String status)
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
	
	
	public static void reportResult(String invertedIndex)
	{
	    Socket soc;
		try 
		{
			System.out.println("report Result has been called : "+Worker.masterhost+"  "+Worker.masterport);
			soc = new Socket(Worker.masterhost,Worker.masterport);	
			DataOutputStream bw = new DataOutputStream(soc.getOutputStream()); 
			StringBuffer queryString = new StringBuffer();
			//queryString.append("?");
			/*queryString.append(AllStrings.portString);
			queryString.append("=");
			queryString.append(Worker.workerport);
			queryString.append("&");
			queryString.append(AllStrings.wordForInvertedIndexString);
			queryString.append("=");
			queryString.append(word);
			queryString.append("&");
			queryString.append(AllStrings.invertedIndexFromWorkerString);
			queryString.append("=");*/
			queryString.append(invertedIndex);
			//System.out.println(queryString);
		    //DataOutputStream bw = new DataOutputStream(soc.getOutputStream()); 
		    StringBuffer sb = new StringBuffer();

			sb.append(AllStrings.httpMethodPOST);
			sb.append("/workerresult");
			sb.append(" HTTP/1.1\r\n");
			sb.append(AllStrings.host);
			sb.append(Worker.masterhost);
			sb.append("\r\n");
			sb.append(AllStrings.contentLengthString);
			sb.append(queryString.length());
			sb.append("\r\n");
			sb.append(AllStrings.contentTypeString);
			sb.append(AllStrings.contentTypeValue);
			sb.append("\r\n");
			sb.append(AllStrings.connectionClosed);
			sb.append("\r\n\r\n");
			sb.append(queryString);
			sb.append("\r\n\r\n");
			bw.writeBytes(sb.toString());
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
