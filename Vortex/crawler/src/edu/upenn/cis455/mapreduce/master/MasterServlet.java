package edu.upenn.cis455.mapreduce.master;

import java.io.*;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import javax.servlet.http.*;

import edu.upenn.cis455.mapreduce.AllStrings;


public class MasterServlet extends HttpServlet {

  static final long serialVersionUID = 455555001;
  public static Master m;

  public void doGet(HttpServletRequest request, HttpServletResponse response) 
       throws java.io.IOException
  {
	int totalCrawlCount = 0;
    response.setContentType("text/html");
    PrintWriter out = response.getWriter();
	out.println("<html>");
	out.println("<head>");
	out.println("<style>");
	out.println("table,th,td");
	out.println("{");
	out.println("editor_deselector : \"NOTanEditor\"");
	out.println("border:1px solid black;");
	out.println("border-collapse:collapse;");
	out.println("}");
	out.println("th,td");
	out.println("{");
	out.println("padding:5px;");
	out.println("}");
	out.println("</style>");
	out.println("</head>");
	out.println("<body>");
	out.println("<center>");
	if(Master.isMapping)
	{
		String totalTimeElaspsed = getTimeDiff(System.currentTimeMillis(),Master.timeOfMapIssued);
		out.println("<p>Start  "+totalTimeElaspsed+"  <p>");
	}
	if(Master.isStopped)
	{
				String totalTimeElaspsed = getTimeDiff(System.currentTimeMillis(),Master.timeOfStopped );
		out.println("<p>Stop  "+totalTimeElaspsed+"  <p>");
	}
	out.println("</center>");
	out.println("<table style=\"border:1;width:300px\">");
	out.println("<tr>");
	out.println("<th>IP:Port</th>");
	out.println("<th>Status</th>");		
	out.println("<th>Keys Written</th>");	
	out.println("<th>Seed count</th>");
	out.println("<th>Creation count</th>");
	out.println("<th>Line Count of BFR</th>");
	out.println("</tr>");
	

	//Looping on hash map starts here
	Iterator<Entry<Integer, WorkerInMaster>> it = Master.workerStatus.entrySet().iterator();
    while (it.hasNext()) 
    {
    	Map.Entry<Integer,WorkerInMaster> pairs = (Map.Entry<Integer,WorkerInMaster>)it.next();
    	if(pairs.getValue().getIsActive() == true)
    	{
			out.println("<tr>");
			//Printing IP:Port combination
			out.println("<td>");
			out.println(pairs.getValue().getWorkerHost()+":"+pairs.getKey());
			out.println("</td>");
			
			//Printing status
			out.println("<td>");
			out.println(pairs.getValue().getStatus());
			out.println("</td>");
			

			
			/*//Printing keys read
			out.println("<td>");
			out.println(pairs.getValue().getKeysRead());
			out.println("</td>");
			*/
			//Printing keys written
			out.println("<td>");
			out.println(pairs.getValue().getKeysWritten());
			out.println("</td>");
			totalCrawlCount += pairs.getValue().getKeysWritten();
			out.println("<td>");
			out.println(pairs.getValue().seedcount);
			out.println("</td>");
			
			out.println("<td>");
			out.println(pairs.getValue().creationcount);
			out.println("</td>");
			
			out.println("<td>");
			out.println(pairs.getValue().lineCountOfBFR);
			out.println("</td>");
    	}
    
    }
	//Looping should end here
	out.println("</table>");
	
	
	out.println("<br>");
	out.println("<center>");
	out.println("Total crawl count "+totalCrawlCount);
	out.println("</center>");
	out.println("<h2>Form</h2>");
	out.println("<form action=\"/status\" method=\"post\">");
	out.println("<table class=\"NOTanEditor\"style=\"width:300px\">");
	out.println("<tr>");
	out.println("<td>Input directory:</td><td><input type=\"text\" name=\"inputdirectory\"></td>");
	out.println("</tr>");
	out.println("<tr>");
	out.println("<td>Number of map threads for each worker:</td><td><input type=\"text\" name=\"noofmapthreads\"></td>");
	out.println("</tr>");
	out.println("<tr>");
	out.println("<td>Seed to start with:</td><td><input type=\"text\" name=\"seedtostartwith\"></td>");
	out.println("</tr>");
	out.println("<tr>");
	out.println("<td>Creation count:</td><td><input type=\"text\" name=\"creationcount\"></td>");
	out.println("</tr>");
	out.println("<tr>");
	out.println("<td><input type=\"submit\" value=\"Submit\"></td>");
	out.println("</tr>");
	out.println("</table>");
	out.println("</form>");
	out.println("<form action=\"/issuestop\" method=\"post\">");
	out.println("<table class=\"NOTanEditor\"style=\"width:300px\">");
	out.println("<tr>");
	out.println("<td><input type=\"submit\" value=\"Stop\"></td>");
	out.println("</tr>");
	out.println("</table>");
	out.println("</form>");
	
	//For indexer
	
	out.println("<form action=\"/indexer\" method=\"post\">");
	out.println("<table class=\"NOTanEditor\"style=\"width:300px\">");
	out.println("<tr>");
	out.println("<td>Access Key:</td><td><input type=\"text\" name=\"indexaccesskey\"></td>");
	out.println("</tr>");
	out.println("<tr>");
	out.println("<td>Secret Key:</td><td><input type=\"text\" name=\"indexsecretkey\"></td>");
	out.println("</tr>");
	out.println("<tr>");
	out.println("<td>Bucket Name:</td><td><input type=\"text\" name=\"indexbucketname\"></td>");
	out.println("</tr>");
	out.println("<tr>");
	out.println("<td>To Upload(Local)</td><td><input type=\"text\" name=\"indexuploadlocal\"></td>");
	out.println("</tr>");
	out.println("<tr>");
	out.println("<td>Input Folder in S3</td><td><input type=\"text\" name=\"indexinputfolderins3\"></td>");
	out.println("</tr>");
	
	out.println("<tr>");
	out.println("<td><input type=\"submit\" value=\"Push For Indexer\"></td>");
	out.println("</tr>");
	out.println("</table>");
	out.println("</form>");
	
	//For ranker
	
	out.println("<form action=\"/ranker\" method=\"post\">");
	out.println("<table class=\"NOTanEditor\"style=\"width:300px\">");
	out.println("<tr>");
	out.println("<td>Access Key:</td><td><input type=\"text\" name=\"rankaccesskey\"></td>");
	out.println("</tr>");
	out.println("<tr>");
	out.println("<td>Secret Key:</td><td><input type=\"text\" name=\"ranksecretkey\"></td>");
	out.println("</tr>");
	out.println("<tr>");
	out.println("<td>Bucket Name:</td><td><input type=\"text\" name=\"rankbucketname\"></td>");
	out.println("</tr>");
	out.println("<tr>");
	out.println("<td>To Upload(Local)</td><td><input type=\"text\" name=\"rankuploadlocal\"></td>");
	out.println("</tr>");
	out.println("<tr>");
	out.println("<td>Input Folder in S3</td><td><input type=\"text\" name=\"rankinputfolderins3\"></td>");
	out.println("</tr>");
	
	out.println("<tr>");
	out.println("<td><input type=\"submit\" value=\"Push For Ranker\"></td>");
	out.println("</tr>");
	out.println("</table>");
	out.println("</form>");
	
	out.println("</body>");
	
	out.println("</html>");

  }
  
  
  public static String getTimeDiff(long time2,long time1)
  {
		long diff = time2-time1;
		long diffSeconds = diff / 1000 % 60;  
		long diffMinutes = diff / (60 * 1000) % 60;         
		long diffHours = diff / (60 * 60 * 1000);  
		return "Elapsed Time: "+diffHours+":"+diffMinutes+":"+diffSeconds;
  }
  
  
  
  public void doPost(HttpServletRequest request, HttpServletResponse response) 
	       throws java.io.IOException
  {
	  String workType = request.getRequestURI();
	  System.out.println(request.getPathInfo());
	  PrintWriter out = response.getWriter();
	  out.println(workType);
	  out.println(request.getPathInfo());
	  if(workType.equals("/status"))
	  {
		  m = new Master();
		  String ipdirectory = request.getParameter("inputdirectory");
		  int noOfMapThreads = Integer.valueOf( request.getParameter("noofmapthreads"));
		  String seedtostartwith = request.getParameter(AllStrings.seedToStartWithString);
		  String creationcount = request.getParameter(AllStrings.creationCountString);
		  
		  m.setFormParameters(ipdirectory, noOfMapThreads,seedtostartwith,creationcount);	  
		  Master.callRunmapFirstTime();
		  Master.isMapping = true;
		  Master.timeOfMapIssued = System.currentTimeMillis();
		 // Master.seedToStartWith = -1;
	  }
	  else if(workType.equals("/issuestop"))
	  {	    
		  Master.issueStop();
		  Master.timeOfStopped =  System.currentTimeMillis();
	  }
	  /*else if(workType.equals("/indexer"))
	  {
		  
	  }
	  else if(workType.equals("/ranker"))
	  {
		  
	  }*/
  }
}
  
