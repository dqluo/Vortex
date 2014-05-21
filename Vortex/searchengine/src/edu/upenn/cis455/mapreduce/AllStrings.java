package edu.upenn.cis455.mapreduce;

import java.util.HashMap;
import java.util.regex.Pattern;

;

public class AllStrings {


	  public  static final String httpVersionString = "HTTP/1.1";	
	  public  static final String contentTypeString = "Content-Type: ";
	  public  static final String contentLengthString = "Content-Length: ";
	  public static  final String contentLengthKeyWord = "Content-Length";
	  public  static final String headerdate ="Date: ";
	  public  static final String lastModified="Last-Modified: ";
	  public  static final String contentTypeValue = "application/x-www-form-urlencoded";
	

      public static boolean isHashMapInitialized = false;

	  public static final String httpServer = "HTTPServer";
	  public static final String servletServlet = "ServletServer";
	 
	  
	  //For request headers
	  public  static final String ifModifiedSince ="If-Modified-Since: ";
	  public  static final String connectionClosed ="Connection: close";
	  public static  final String userAgent        ="User-Agent: cis455crawler";
	  public static final  String host ="Host: ";
	  public static final  String httpMethodGET ="GET ";
	  public static final  String httpMethodHEAD ="HEAD ";
	  public static final String httpMethodPOST = "POST ";
	  public static final String location ="Location: ";
	  
 
	  //For parsing response headers
	  public static final String twohundredOK = "200";
	  public static final String threeHundredNotModified = "304";
	  public static final String threeHundredFound ="302";
	  public static final String threeHundredMovedPermanently = "301";
	  

	  

	  //For url
	  public static final String http ="http://"; 

	  //For queryString
	  public static final String portString = "port";
	  public static final String statusString = "status";
	  public static final String jobnameString = "job";
	  public static final String crawlcountString = "crawlcount";
	  
	  //For runmap post
	  public static final String inputDirectoryString = "input";
	  public static final String numOfWorkersString = "numThreads";
	  public static final String numOfThreadsString = "numWorkers";
	  public static final String seedToStartWithString = "seedtostartwith";
	  public static final String creationCountString = "creationcount";
	  public static final String lineReadTillInBufferedReaderString = "linereadtill";
	  public static final int maxDataToBeRead = 1024*1024;
	  
	  
	  //Separators for sending the index
	  
	  public static final String reduceSeperator ="<@@@!REDUCESEPARATOR!@@@>";
	  public static final String urlSeperator ="<@!Separatorurl!@>";
	  public static final String newSeperator ="<@!NEWSEPARATOR!@>";
	  
	  //Workers status
	  public static final String requested = "requested";
	  public static final String sent = "sent";
	  public static final String idle ="idle";
	  
	  //For searcg engine
	  
	  public static final String wordForInvertedIndexString = "wordforinvertedindex";
	  public static final String invertedIndexFromWorkerString = "invertedindexfromworker";
	  
	  
	  public static final String stopWordsarr[]={"i","me","my","myself","we","our","ours","ourselves","you","your","yours","yourself","yourselves","he","him","his","himself","she","her","hers","herself","it","its","itself","they","them","their","theirs","themselves","what","which","who","whom","this","that","these","those","am","is","are","was","were","be","been","being","have","has","had","having","do","does","did","doing","a","an","the","and","but","if","or","because","as","until","while","of","at","by","for","with","about","against","between","into","through","during","before","after","above","below","to","from","up","down","in","out","on","off","over","under","again","further","then","once","here","there","when","where","why","how","all","any","both","each","few","more","most","other","some","such","no","nor","not","only","own","same","so","than","too","very","s","t","can","will","just","don","should","now"};
	  
	  public static final HashMap<String,Boolean> stopWords = new HashMap<String,Boolean>();
	  
	  public static Pattern acceptedPatternOfKeyWords = Pattern.compile("[a-z]*");
	  
	  public static void constructStopWordHashMap()
	  {
		  isHashMapInitialized = true;
		  for(int i=0;i<stopWordsarr.length;i++)
		  {
			  stopWords.put(stopWordsarr[i], true);
		  }
	  }
	  
	
}


