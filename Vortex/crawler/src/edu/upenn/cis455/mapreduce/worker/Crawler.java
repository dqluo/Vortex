package edu.upenn.cis455.mapreduce.worker;

import java.util.HashMap;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.net.URL;

import javax.net.ssl.HttpsURLConnection;


import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import org.jsoup.select.Elements;



import edu.upenn.cis455.mapreduce.CrawlCount;
import edu.upenn.cis455.mapreduce.WorkerContext;
import edu.upenn.cis455.mapreduce.storage.CrawlerStore;

public class Crawler 
{
	   public String startURL;
	   public String berdir;
	   public static String contentTypeOfCurrentDocument = null;
	   public static CrawlerStore cs;
	   public String url302 = null;
	   public boolean is302 = false;
	   public static int globalCount =0;
	   Socket soc;
	   public CrawlCount ccjob = new CrawlCount();
	   public WorkerContext wc = new WorkerContext();
	   public static String lastCrawledURL;
	   public boolean isHttps = false;
	   static Logger logger = Logger.getLogger(Crawler.class);
	   

	   public Crawler()
	   {		   
		   PropertyConfigurator.configure("Log/log4j.properties");
	   }
	   public boolean checkRobot(String host,String url,String urlHash,String SHAhost)
	   {	
		   //TODO find out what to allow properly		   
		   try
		   {
			   if(cs.shouldDisallow(SHAhost,url))
			   {
					if(cs.shouldAallow(SHAhost, url))
						return true;
					else 
					{
						return false;
					}
			   }
			   Long crawldelay = cs.retrieveFromHostIdCrawlDelayDB(SHAhost);
			   if(crawldelay != null)
			   {
				   if(crawldelay.compareTo(new Long(0)) == 0)
				   {
					   
				   }
				   else
				   {
					   Long whenLastCrawled =  cs.retrieveFromHostIdWhenLastCrawledDB(SHAhost);
					   if(whenLastCrawled ==  null)
					   {}
					   else
					   {
						   Long currentTime=System.currentTimeMillis();
						   Long howlong = currentTime-whenLastCrawled;
						   Long wait = howlong - (crawldelay*1000);
						   if(wait >= 0)
						   {}
						   else
						   {
							   Long howmuch = crawldelay*1000 - howlong;
							   //If crawl delay is greater than 5 seconds then map it back 
							   
							   if(howmuch >= 5000)
							   {
								   ccjob.map(url, "", wc);
								   return false;
							   }
							   else
							   {
								   //Sleep for that much time
								   Thread.sleep(howmuch);
							   }
							   
						   }
					   }
				   }
			   }
			   else
			   {
				   Robots rb = new Robots(host,cs,SHAhost);
				   rb.parseRobotsTXT(rb.getRobotsTXT());
			   }
			   

		   }
		   catch(Exception e)
		   {
			   logger.error("Error in function checkrobot for "+url+" and error message is: "+e);
		   }

			return true;
			 
	   }

	   public boolean makeHEADRequest(String url,String urlHash,String protocol,String host,String filename,String SHAhost)
	   { 
		 try
		 {	   
		   String currentURL = url;		  
		   //System.out.println(url.toString());
		   if(!protocol.toLowerCase().equals(AllStringsCrawler.http))
		   {
		      if(!protocol.toLowerCase().equals(AllStringsCrawler.https))
				   return false;
		      else
		      {
		    	  if(cs.retrieveFromURLIdURL(urlHash) != null)
		    		  return false;
		      }
		   }
		   String inputLine;
		   Socket soc;	
		   if(checkRobot(host,currentURL,urlHash,SHAhost) == false)
		   {
			   logger.info("For url: "+currentURL+" :ROBOT.TXT Disallow");
			   return false;
		    }
			BufferedReader in = null;
			try 
			{	
				//Constructing the request
				if(protocol.toLowerCase().equals(AllStringsCrawler.http))
				{
					StringBuffer headers = new StringBuffer();
					headers.append(AllStringsCrawler.httpMethodHEAD +filename+" "+AllStringsCrawler.httpVersionString+"\r\n");
					headers.append(AllStringsCrawler.host+host+"\r\n");
					headers.append(AllStringsCrawler.userAgent+"\r\n");
					String uc = cs.retrieveFromURLIdURL(urlHash);	
					if(uc != null)
					{		
						logger.info(currentURL+" : Already in database.");
						return false;
					}
					headers.append(AllStringsCrawler.connectionClosed+"\r\n\r\n");
					soc = new Socket(host,80);
					soc.setSoTimeout(10000);
					DataOutputStream bw = new DataOutputStream(soc.getOutputStream()); 
					bw.writeBytes(headers.toString());
					bw.flush();				
					in = new BufferedReader(new InputStreamReader(soc.getInputStream()));
				
				inputLine = in.readLine();
				//Check for 200 ok
				inputLine = inputLine.split(" ")[1];
				//System.out.println("Status code"+inputLine);
				if(!inputLine.equals(AllStringsCrawler.twohundredOK))
				{
					if(inputLine.equals(AllStringsCrawler.threeHundredFound) || inputLine.equals(AllStringsCrawler.threeHundredMovedPermanently))
					{
						is302 = true;
						logger.info(currentURL+" : 302 Found");

					}
					else if(inputLine.equals(AllStringsCrawler.fourHundred) ||
							inputLine.equals(AllStringsCrawler.fourHundredfour))
					{
						if(!url.substring(7).contains("/"))
						{
							url = url +"/";							
							ccjob.map(url, "", wc);
							return false;
						}
					}
					else
					{
						logger.info(currentURL+" : "+inputLine);
						return false;
					}
					
					
				}						
				while ((inputLine = in.readLine()) != null)
				{ 	 
					
					//Checking content type
					if(inputLine.startsWith(AllStringsCrawler.contentTypeString))
					{
						//TODO content type check add code here for PDF's
						contentTypeOfCurrentDocument = inputLine.substring(14).trim();
						if(!contentTypeOfCurrentDocument.toLowerCase().contains("text/html"))
						  // if(!contentTypeOfCurrentDocument.toLowerCase().endsWith("xml"))
							{
							    logger.info(currentURL+" : Not HTML");
							    if(!soc.isClosed())
							    	soc.close();
								return false;
							}
					 }
					if(inputLine.contains("Content-language"))
					  {
						  String langType=inputLine.split(":")[1].trim();
						  if(!langType.contains("en"))						  
						  {
							  logger.info(currentURL+" : Content- Language is not english ");
							  return false;
						  }
					  }
					//Checking location header for 
					if(is302)
					{
						if(inputLine.startsWith(AllStringsCrawler.location))
						{
							url302 = inputLine.substring(10).trim();
							if(!url302.startsWith(AllStringsCrawler.https) || !url302.startsWith(AllStringsCrawler.http))
							{
								URL baseUrl = new URL(currentURL);
								URL urlcorrect = new URL( baseUrl , url302);
								url302 = urlcorrect.toString();
							}
							//TODO log
							//Loop in redirection
							if(url302.equals(currentURL))
							{
								//TODO get via HTTP connection
								is302 = false;
								url302 = null;
								logger.info(currentURL+" : Loop redirection ");
								return false;
							}
						}
					}
					if(inputLine.isEmpty())
						break;
				}	
				if(contentTypeOfCurrentDocument ==  null)
				{
					if(!soc.isClosed())
						soc.close();
					return false;
				}
				if(!soc.isClosed())
					soc.close();
				return true;
			    } 
				else 
				{
					URL u = new URL(currentURL);
					HttpsURLConnection urlConn = (HttpsURLConnection) u.openConnection();
					urlConn.setRequestMethod("HEAD");
					urlConn.setRequestProperty("User-Agent", AllStringsCrawler.cis455UserAgent);
					contentTypeOfCurrentDocument = urlConn.getContentType();
					String responseCodeOfHTTPS = String.valueOf(urlConn.getResponseCode());
					if(!responseCodeOfHTTPS.equals(AllStringsCrawler.twohundredOK))
						return false;
					if(!contentTypeOfCurrentDocument.toLowerCase().contains("text/html"))
						return false;
					
					
				}
			}
			catch (Exception e)
			{
				logger.error("Error when sending head request to: "+url+" and the error message is: "+e);
				return false;
			} 
		 }
		 catch(Exception e)
		 {
			 
			 logger.error("Error when manipulating the url to: "+url+" and the error message is: "+e);
			return false;
		 }
		return true;
		   
	   }
   //Make GET Request 
	   public BufferedReader makeGETRequest(String url,String host,String filename)
	   {
		   URL u;
		   try
		   {
			u = new URL(url);
		    u.getHost();
			BufferedReader in = null;
			try 
			{				
				//Constructing the request
				StringBuffer req = new StringBuffer();
				req.append(AllStringsCrawler.httpMethodGET +filename+" "+AllStringsCrawler.httpVersionString+"\r\n");
				req.append(AllStringsCrawler.host+host+"\r\n");
				req.append(AllStringsCrawler.userAgent+"\r\n");
				req.append(AllStringsCrawler.connectionClosed+"\r\n\r\n");
				soc = new Socket(host,80);
				soc.setSoTimeout(10000);
				DataOutputStream bw = new DataOutputStream(soc.getOutputStream()); 
				bw.writeBytes(req.toString());
				bw.flush();				
				in = new BufferedReader(new InputStreamReader(soc.getInputStream()));
			} 
			catch (IOException e)
			{
				logger.error("Error when sending GET request to: "+url+" and the error message is: "+e);	
			} 			
				return in;		   
		    } 
		    catch (Exception e)
		    {
			//TODO log
		    	logger.error("Error when manipulating the url to: "+url+" and the error message is: "+e);
				
			return null;
		}
	   }
   public void openEnvir()
   {
	   cs = new CrawlerStore(berdir);
   }
   public void openDB()
   {
	  
	   cs.openDBTables();
   }
   public void closeDB()
   {
	   try
	   {
		   if(soc != null)
		   {
			   if(!soc.isClosed())
				   soc.close();
		   }
	   } 
	   catch (IOException e)
		{
			logger.error("Error with socket. "+e);
		}
	   cs.closeDBTables();
	   
   }
   public void closeEnv()
   {
	   cs.closeEnvironment();
   }
    public void crawlAndStoreURL(String url)
    {
       try 
       {
       String currentDocBeingCrawled = url;
       String htmlContent = null;
	   URL u;
	   u = new URL(url);
	   String protocol = u.getProtocol();
	   String URLHash = AllStringsCrawler.convertToSHAHash(currentDocBeingCrawled);
	   String filename = u.getFile();
	   String host = u.getHost();
	   String SHAhost = AllStringsCrawler.convertToSHAHash(host);
	   if(makeHEADRequest(currentDocBeingCrawled,URLHash,protocol,host,filename,SHAhost))
	   {
		   if(is302)
		   {
			   is302= false;
			   logger.info("Redirected url from "+currentDocBeingCrawled+" to "+url302);
			   currentDocBeingCrawled = url302;
			   ccjob.map(url302, "", wc);
			   //TODO write url302 to respective file
			   url302 = null;
			   return;
		   }
		   else if(u.getProtocol().toLowerCase().equals(AllStringsCrawler.https))
		   {
			   isHttps = false;
			   htmlContent = parseResponse(makeHTTPSGet(u));
			   
		   }
		   else
		   {
			   htmlContent = parseResponse(makeGETRequest(currentDocBeingCrawled,host,filename));
			   if(!soc.isClosed())
				 soc.close();
		   }
		   if(htmlContent==null)
		   {
			   logger.error("The response is null for "+currentDocBeingCrawled);
			   return;
		   }
		   insertIntoURLDatabase(currentDocBeingCrawled,htmlContent,SHAhost,URLHash);
		   logger.info(currentDocBeingCrawled +":Downloading");
		   parseDocumentAndInsertAllURL(currentDocBeingCrawled,htmlContent,URLHash);
	   }
   	   } 
       catch (Exception e) 
       {
		// TODO log

       }
    }
    synchronized public void insertIntoURLDatabase(String url,String content,String SHAhost,String urlid)
    {
 	   //TODO use already converted hash seconds
 	   //Hashing URLId
       globalCount++;
       lastCrawledURL = url;
 	   try
 	   {
 	   //Getting checksum from the document
 	   String docchecksum = AllStringsCrawler.checksum(content);
 	   
 	   cs.insertIntoURLIdURL(urlid, url);
 	   
 	   //Inserting URL ID and content
 	   cs.insertIntoDocIdDocContentDB(docchecksum, content);
 	   
 	   //Inserting URL ID and document checksum
 	   cs.insertIntoURLIdDocIdDB(urlid, docchecksum);
 	   
 	   //Inserting URL Id and when last crawled
 	  cs.insertIntoHostIdWhenLastCrawledDB(SHAhost, System.currentTimeMillis());
 	   
 	   //Inserting DocId and URL Id concatenated and separated with semi colons
 	   cs.insertIntoDocIdURLIdToDB(docchecksum, urlid);
 	   }
 	   catch(Exception e)
 	   {
 		   logger.error("Error when inserting "+url+" into the database: "+e);
 	   }
 	   
    }
       public BufferedReader makeHTTPSGet(URL url)
       {
	   		HttpsURLConnection urlConn;
			try 
			{
				urlConn = (HttpsURLConnection) url.openConnection();
				urlConn.setRequestMethod("GET");
				urlConn.setRequestProperty("User-Agent",AllStringsCrawler.cis455UserAgent);
				
				urlConn.connect();
				BufferedReader in = new BufferedReader(new InputStreamReader(urlConn.getInputStream()));
				return in;
			} 
			catch (Exception e)
			{
				// TODO log
			}
			return null;
       }
       
	   //Parses Response
	   public String parseResponse(BufferedReader in)
	   {		    
			String inputLine=""; 
			StringBuffer sb = new StringBuffer(); 			
			try 
			{
				inputLine = in.readLine();
				while ((inputLine = in.readLine()) != null)
				{ 	 
				    if(inputLine.isEmpty())
						break;
				}		
				while ((inputLine = in.readLine()) != null) 
				{
						sb.append(inputLine);
				}
			} 
			catch (IOException e)
			{
				logger.error("Error when parsing response for: "+inputLine+" and error is: "+e);
				return null;
			} 
		  return sb.toString();
	   }
	   
    //Gets document
    public Document getDocument(String baseURI,String parsedContent)
    {
 	   try
 	   {
 		   Document docjsoup = null;
 		   docjsoup = Jsoup.parse(parsedContent,baseURI);
 		   return docjsoup;
 	   }
 	   catch(Exception e)
 	   {
 		   //TODO log
 		   logger.error("Error in creating the doc from jsoup. error message is: "+e);
			   return null;
 	   }
    }
    public void parseDocumentAndInsertAllURL(String currentURL, String htmlContent, String URLHash)
    {
 	   Document doc = getDocument(currentURL,htmlContent);
 	   //So that the value is not preserved
 	   contentTypeOfCurrentDocument = null;
 	   if(doc == null)
 	   {
 		   logger.info("Could not add url: "+currentURL);
 		   return;
 	   }
 	   try
 	   {
 		   	   HashMap<String,Boolean> toLinksCheck = new HashMap<String,Boolean>();
	    	   StringBuffer toLinks = new StringBuffer();
	           Elements links = doc.select("a[href]");         
	           Elements imports = doc.select("link[href]");     	   
	    	   for(Element link: links)
	   		   {
		       		//Ignore if relative link starts with #
		       		//TODO log
	    		   String abslink = link.attr("abs:href").toString();
	       		   String relHref = link.attr("href");
	       		   if(!relHref.startsWith("#"))
		    		   if(!abslink.isEmpty())
		   			   {
		    			   ccjob.map(abslink, "",wc);
		    			   if(!toLinksCheck.containsKey(abslink))
		    			   {
		    				   toLinks.append(AllStringsCrawler.convertToSHAHash(abslink));
		    				   toLinks.append(";");
		    				   toLinksCheck.put(abslink, true);
		    			   }
		   			   }
			   }
	    	  for(Element link: imports)
	   		  {
	   			
	    		String abslink1 = link.attr("abs:href").toString();
	       		String relHref = link.attr("href");
	       		//Ignore if relative link starts with #
	       		//TODO log
	       		if(!relHref.startsWith("#"))
		   			if(!abslink1.isEmpty())
		   			{
		   				ccjob.map(abslink1, "",wc);
	    			    if(!toLinksCheck.containsKey(abslink1))
	    			    {
	    			       toLinks.append(AllStringsCrawler.convertToSHAHash(abslink1));
	    				   toLinks.append(";");
	    				   toLinksCheck.put(abslink1, true);
	    			    }
		   			}
			  }  
	    	  synchronized(cs)
	    	  {
	    		  if(!toLinks.toString().isEmpty())
	    		  {
	    			 
	    			  cs.insertIntoURLIDURLToDB(URLHash, toLinks.toString());
	    		  }
	    	  }
 	  
 	   }
 		 catch(Exception e)
 		 {
 			 logger.error("Error while retreving links from "+currentURL+" message is: "+e);
 			 return;
 		 }
 	  
    }
}
