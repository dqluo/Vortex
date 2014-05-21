package edu.upenn.cis455.mapreduce.master;

import java.io.*;
import java.net.URL;
import java.util.ArrayList;
import java.util.StringTokenizer;

import javax.servlet.ServletException;
import javax.servlet.http.*;

import shortestEdit.ShortestEdit;

import edu.upenn.cis455.mapreduce.AllStrings;


public class MasterServlet extends HttpServlet {

  static final long serialVersionUID = 455555001;
  public static Master m;
  String searchInput;
  public void doGet(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		//Set response content type
		response.setContentType("text/html");
		PrintWriter out = response.getWriter();

		out.println("<html>");
		out.println("<link href='WEB-INF/resources/css/stylesheet.css' rel='stylesheet' type='text/css'");	//THIS LINE
		out.println("<head><title>Vortex</title></head>");
		out.println("<body>");
		out.println("<div id='mainPage'>");
        out.println("<center>");
		out.println("<h1>Vortex</h1>");

		out.println("<h2 id='homeForm'>");
		out.println("<form action=\"/status\" method=\"post\">");
		out.println("<input type='text' name='querystring' x-webkit-speech ><br>");  //Search bar
		out.println("<input type='submit' name='resultSubmit' value='Give it a whirl!'>");	//resultSearch button
		out.println("</form>");
		/*out.println("<form action='status'>");
		out.println("<input type='text' name='Input' x-webkit-speech ><br>");  //Search bar
		out.println("<input type='submit' name='pictureSubmit' value='Vortex Image!'>");	//pictureSearch button
		out.println("</form>");*/
		out.println("</h2>");
		out.println("</div>");
		out.println("</body>");
		out.println("</html>");

	}
  
  public void doPost(HttpServletRequest request, HttpServletResponse response) 
	       throws java.io.IOException
  {
	PrintWriter out = response.getWriter();
	long starttime = System.currentTimeMillis();
	String queryString = request.getParameter("querystring");
	if(!AllStrings.isHashMapInitialized)
		AllStrings.constructStopWordHashMap();
	
	Master.splitAndHash(queryString);
	
	//Call all functions after fetching inverted indices
	
	Master.startFetchingResults();
    long endtime = System.currentTimeMillis();
        out.println("Time taken to fetch results "+ (endtime - starttime));
  
 /* out.println("Time taken to fetch results "+ (endtime - starttime));
	 for (int i=0;i<Master.urlsToDisplay.size();i++)
	 {
        out.println(Master.urlsToDisplay.get(i));
	 } */
    
	//homePage homePage = new homePage();
	 //searchInput = homePage.searchInput;

	//Set response content type
	response.setContentType("text/html");

	//PrintWriter out = response.getWriter();

	out.println("<html>");
	out.println("<head>");
	out.println("<title>Vortex</title>");
	out.println("<link href='/WEB-INF/resources/css/stylesheet.css' rel='stylesheet' type='text/css'");
/*	out.println("<script type='text/javascript' src='/js/jquery-live-preview.js'></script>");
	out.println("<style>" + 
	"#frame {"+
	   "width: 800px;"+
	    "height: 520px;"+
	    "border: none;"+
	    "-moz-transform: scale(0.2);"+
	    "-moz-transform-origin: 0 0;"+
	    "-o-transform: scale(0.2);"+
	    "-o-transform-origin: 0 0;"+
	    "-webkit-transform: scale(0.2);"+
	    "-webkit-transform-origin: 0 0;"+
	"}"+
	"</style>");*/
	out.println("</head>");
	out.println("<body>");
	
	out.println("<div id='resultPage'>");
	
	out.println("<div id='topNav'>");
	out.println("<ul>");
	out.println("<li><a href='/status'>Home</a></li>");
//	out.println("<li><a href='/status'>Images</a></li>");	//correct location?
	out.println("</ul>");
	out.println("</div>");
	
	out.println("<h1>");
	out.println("Vortex");
	out.println("<form action='/status' method=\"POST\"> <input type='text' name='querystring' x-webkit-speech >");
	out.println("<input type='submit' name='newResultSearch' value='Give it a Whirl!'>");
	out.println("</form>");
	out.println("</h1>");
	
	searchInput = request.getParameter("querystring");
	ShortestEdit se = new ShortestEdit();
	StringTokenizer st = new StringTokenizer(searchInput);
	int numWords = st.countTokens();
//        out.println("Time taken to fetch results "+ (endtime - starttime));

	while(st.hasMoreElements()) {
		String word = st.nextElement().toString();
		word = word.replaceAll("\\p{P}", "");
		String rightWord = se.getShortestEdit(word.toLowerCase());
		if(!(rightWord.equals(word.toLowerCase()))) {
			out.println("<br>Did you mean: " + rightWord);
		}
		else {
			//do nothing
		}
	}
    
	//out.println("Time taken to fetch results "+ (endtime - starttime));
	/*
	ArrayList<String> info = new ArrayList<String>();
	info.add("http://www.bing.com");
	info.add("http://www.yahoo.com");
	info.add("http://www.youtube.com");
	info.add("http://www.ebay.com");
	info.add("http://www.google.com");*/

/*	int listLength = info.size();

	out.println("<div id='searchResults'>");
	out.println("<p>Search Results for: " + searchInput.replaceAll("\\p{P}", "") + "</p>");
	int i=0;
	while(i<listLength) {
		if(searchInput != null) {
			String url = info.get(i);
			out.println(i + ".  <a href='"+url+"'>" + info.get(i) + "</a> <br><br>");
		}
		i++;
	}*/
	 if(!Master.urlsToDisplay.get(0).contains("Sorry! There are no results for your query!")){
	out.println("<iframe id='frame' height='700px' width = '500px' align='right' src='"+ Master.urlsToDisplay.get(0) +"'></iframe>");
	}
	
	int listLength = Master.urlsToDisplay.size();

	out.println("<div id='searchResults'>");
	out.println("<p>Search Results for: " + searchInput.replaceAll("\\p{P}", "") + "</p>");
	int i=0;
	String firstURL = "";
	if(Master.urlsToDisplay.get(0) != null) {
		firstURL = Master.urlsToDisplay.get(0);
	}
	
	while(i<listLength) {
		if(searchInput != null) {
			String url = Master.urlsToDisplay.get(i);
			//if(url.contains("acupuncture"))
			//	continue;
			if(!url.contains("Sorry! There are no results for your query!")){
			URL findhost = new URL(url);
			String hostName = findhost.getHost();
			out.println(hostName);
			}
			out.println("<a href='"+url+"'>" + Master.urlsToDisplay.get(i) + "</a> <br><br>");
		}
		i++;
	}
	//iintln("<iframe id='frame' src='"+ firstURL +"'>");
	
	
	
	
	out.println("</div>");
	/*
	out.println("<div id='bottomNav'>");
	out.println("<ul>");
	//out.println("<li><a href='homePage'>Home</a></li>");
	out.println("<li><a href='resultPage2'>Next</a></li>");
	out.println("</ul>");
	out.println("</div></div>");*/
	
	out.println("</body>");
	out.println("</html>");
	 
	 Master.clearAllHashMaps();
  }
}
