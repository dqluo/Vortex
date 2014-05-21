package edu.upenn.cis455.mapreduce;

;

public class AllStrings {


	  public  static final String httpVersionString = "HTTP/1.1";	
	  public  static final String contentTypeString = "Content-Type: ";
	  public  static final String contentLengthString = "Content-Length: ";
	  public static  final String contentLengthKeyWord = "Content-Length";
	  public  static final String headerdate ="Date: ";
	  public  static final String lastModified="Last-Modified: ";
	  public  static final String contentTypeValue = "application/x-www-form-urlencoded";
	



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
	  
	  public static final String accessKeyStringIndex ="indexaccesskey";
	  public static final String secretKeyStringIndex ="indexsecretkey";
	  public static final String bucketNameStringIndex="indexbucketname";
	  public static final String pathinS3StringIndex = "indexpathins3";
	  public static final String uploadlocalStringIndex ="indexuploadlocal";
	  public static final String inputFolderinS3StringIndex ="indexinputfolderins3";
	  
	  public static final String accessKeyStringRank ="rankaccesskey";
	  public static final String secretKeyStringRank ="ranksecretkey";
	  public static final String bucketNameStringRank="rankbucketname";
	  public static final String pathinS3StringRank = "rankpathins3";
	  public static final String uploadlocalStringRank ="rankuploadlocal";
	  public static final String inputFolderinS3StringRank ="rankinputfolderins3";
	  
	  public static final String myWorkerIDString = "myid";

	  
	  
	
}


