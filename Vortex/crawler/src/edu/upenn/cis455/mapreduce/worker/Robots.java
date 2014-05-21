package edu.upenn.cis455.mapreduce.worker;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.net.URL;
import java.util.ArrayList;
import edu.upenn.cis455.mapreduce.storage.CrawlerStore;

public class Robots 
{	
	public String host;
	public String crawlDelayForStar = "0";
	public String crawlDelayForCIS455 = "0";
	public ArrayList<String> alAllowForCIS455 = new ArrayList<String>();
	public ArrayList<String> aldisallowForCIS455 = new ArrayList<String>();
	public ArrayList<String> alAllowForStar = new ArrayList<String>();
	public ArrayList<String> aldisallowForStar = new ArrayList<String>();
	public CrawlerStore cs;
	public String hostHash;
	public Socket soc;
	String sep = "!!!!!@@@@@@@@!!!!!!@@@@@@!!!!!@@@!!!";
	
	public Robots(String host,CrawlerStore cs,String hostHash)
	{
		this.host = host.trim();
		this.cs = cs;
		this.hostHash = hostHash;
		
	}
	
	@SuppressWarnings("resource")
	public void parseRobotsTXT(BufferedReader in)
	{		
		String inputLine; 			
		try 
		{
			String ua = null;
			inputLine = in.readLine();
			//Ignore headers
			inputLine = inputLine.split(" ")[1];
			if(!inputLine.equals(AllStringsCrawler.twohundredOK))
				return;
			while ((inputLine = in.readLine()) != null)
			{ 	 
		
				if(inputLine.isEmpty())
					break;
			}		
			//Read content line by line
			while ((inputLine = in.readLine()) != null) 
			{ 
				//If it starts with use agent
				if(inputLine.startsWith(AllStringsCrawler.robotUserAgent))
				{

						ua = inputLine.split(":")[1].trim();
						if(ua.equals("*"))
						{
							in = readAllForAllowedUserAgents(in,ua);
							
						}
						else if(ua.equals(AllStringsCrawler.cis455UserAgent))
						{
							in = readAllForAllowedUserAgents(in,ua);
						}
				}
			}
			in.close();
			if(!soc.isClosed())
				soc.close();
			//Update to database
			writeRobotsInfoTODB();
		} 
		catch (IOException e)
		{
			//System.out.println("Exception while parsing the robots.txt"+e);
		
		} 
	}
	
	public BufferedReader readAllForAllowedUserAgents(BufferedReader in,String ua)
	{
		String inputLine;
		try 
		{
			//Disallow and allow for *
			if(ua.equals("*"))
			{
				while ((inputLine = in.readLine()) != null) 
				{ 
					
					if(inputLine.startsWith(AllStringsCrawler.allow))
					{
						String add = inputLine.split(":")[1].trim();
						URL hostURL = new URL("http://"+host);
						URL allowURL = new URL(hostURL, add);
						alAllowForStar.add(allowURL.toString());
						
					}
					else if(inputLine.startsWith(AllStringsCrawler.disallow))
					{
						String add = inputLine.split(":")[1].trim();
						if(add.isEmpty())
						{
							aldisallowForStar.clear();
							break;
						}
						URL hostURL = new URL("http://"+host);
						URL disallowURL = new URL(hostURL, add);
						aldisallowForStar.add(disallowURL.toString());
					}
					else if(inputLine.startsWith(AllStringsCrawler.crawldelay))
					{
						crawlDelayForStar = inputLine.split(":")[1].trim();
						
					}
					else
						return in;
				}
					
			}
			//Disallow and allow for CIS455
			else if(ua.equals(AllStringsCrawler.cis455UserAgent))
			{
				while ((inputLine = in.readLine()) != null) 
				{ 
					
					if(inputLine.startsWith(AllStringsCrawler.allow))
					{
						String add = inputLine.split(":")[1].trim();
						URL hostURL = new URL("http://"+host);
						URL allowURL = new URL(hostURL, add);
						alAllowForCIS455.add(allowURL.toString());
						
					}
					else if(inputLine.startsWith(AllStringsCrawler.disallow))
					{
						String add = inputLine.split(":")[1].trim();
						if(add.isEmpty())
						{
							aldisallowForCIS455.clear();
							break;
						}
						URL hostURL = new URL("http://"+host);
						URL disallowURL = new URL(hostURL, add);
						aldisallowForCIS455.add(disallowURL.toString());
					}
					else if(inputLine.startsWith(AllStringsCrawler.crawldelay))
					{
						crawlDelayForCIS455 = inputLine.split(":")[1].trim();
						
					}
					else
						return in;
				}
					
			}
		} 
		catch (IOException e)
		{
			//System.out.println("Exception while parsing the robots.txt"+e);
		}
		
		return in;
	}
	
	
	public BufferedReader getRobotsTXT()
	{
		
	    String filename = "/robots.txt";
	    if(host.endsWith("/"))
	    	filename = "robots.txt";
		BufferedReader in = null;
		try 
		{
			soc = new Socket(host,80);
			soc.setSoTimeout(10000);
			DataOutputStream bw = new DataOutputStream(soc.getOutputStream()); 
			//Constructing the request
			bw.writeBytes(AllStringsCrawler.httpMethodGET +filename+" "+AllStringsCrawler.httpVersionString+"\r\n");
			bw.writeBytes(AllStringsCrawler.host+host+"\r\n");
			bw.writeBytes(AllStringsCrawler.userAgent+"\r\n");
			bw.writeBytes(AllStringsCrawler.connectionClosed+"\r\n\r\n");				
			bw.flush();				
			in = new BufferedReader(new InputStreamReader(soc.getInputStream()));
			
		} 
		catch (IOException e)
		{
			//	System.out.println("Exception while sending a request to robots .txt");
				//System.out.println("host "+host);
				//System.out.println("Filename "+filename);
		} 
		return in;
	}
	
	public void writeRobotsInfoTODB()
	{

		//For disallow
		if(!aldisallowForCIS455.isEmpty())
		{
			try
			{
				long d = Long.parseLong(crawlDelayForCIS455);
				cs.insertIntoHostIdCrawlDelayDB(hostHash, d);
			}
			catch(Exception e)
			{
				cs.insertIntoHostIdCrawlDelayDB(hostHash, new Long(0));
			}
			StringBuffer semicolonSeperatedDisallowURLS = new StringBuffer();
			for(String dis:aldisallowForCIS455)
			{
				semicolonSeperatedDisallowURLS.append(dis);
				semicolonSeperatedDisallowURLS.append(sep);
			}
			cs.insertIntoHostIdDisallow(hostHash, semicolonSeperatedDisallowURLS.toString().trim());
		}
		else 
		{
			try
			{
				long d = Long.parseLong(crawlDelayForStar);
				cs.insertIntoHostIdCrawlDelayDB(hostHash, d);
			}
			catch(Exception e)
			{
				cs.insertIntoHostIdCrawlDelayDB(hostHash, new Long(0));
			}
			if(!aldisallowForStar.isEmpty())
			{
				StringBuffer semicolonSeperatedDisallowURLS = new StringBuffer();
				for(String dis:aldisallowForStar)
				{
					semicolonSeperatedDisallowURLS.append(dis);
					semicolonSeperatedDisallowURLS.append(sep);
				}
				cs.insertIntoHostIdDisallow(hostHash, semicolonSeperatedDisallowURLS.toString().trim());
			}
			
		}
		//For allow
		if(!alAllowForCIS455.isEmpty())
		{
			StringBuffer semicolonSeperatedAllowURLS = new StringBuffer();
			for(String dis:alAllowForCIS455)
			{
				semicolonSeperatedAllowURLS.append(dis);
				semicolonSeperatedAllowURLS.append(sep);
			}
			cs.insertIntoHostIdAllow(hostHash, semicolonSeperatedAllowURLS.toString().trim());
		}
		else 
		{
			if(!alAllowForStar.isEmpty())
			{
				StringBuffer semicolonSeperatedAllowURLS = new StringBuffer();
				for(String dis:alAllowForStar)
				{
					semicolonSeperatedAllowURLS.append(dis);
					semicolonSeperatedAllowURLS.append(sep);
				}
				cs.insertIntoHostIdAllow(hostHash, semicolonSeperatedAllowURLS.toString().trim());
			}
			
		}
	}
	
}


