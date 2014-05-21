package edu.upenn.cis455.mapreduce.worker;


import java.io.ByteArrayInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;

import java.util.TimeZone;
import java.util.zip.Adler32;
import java.util.zip.CheckedInputStream;



public class AllStringsCrawler {


	  public  static final String httpVersionString = "HTTP/1.1";	
	  public  static final String contentTypeString = "Content-Type: ";
	  public  static final String contentLengthString = "Content-Length: ";
	  public static final String contentLengthKeyWord = "Content-Length";
	  public  static final String headerdate ="Date: ";
	  public  static final String lastModified="Last-Modified: ";
	
	  public  static final String serverName ="Server: LAS";



	  public static final String httpServer = "HTTPServer";
	  public static final String servletServlet = "ServletServer";
	 
	  
	  //For request headers
	  public  static final String ifModifiedSince ="If-Modified-Since: ";
	  public  static final String connectionClosed ="Connection: close";
	  public static  final String userAgent        ="User-Agent: cis455crawler";
	  public static final  String host ="Host: ";
	  public static final  String httpMethodGET ="GET ";
	  public static final  String httpMethodHEAD ="HEAD ";
	  public static final String location ="Location: ";
	  
 
	  //For parsing response headers
	  public static final String twohundredOK = "200";
	  public static final String threeHundredNotModified = "304";
	  public static final String threeHundredFound ="302";
	  public static final String threeHundredMovedPermanently = "301";
	  public static final String fourHundred = "400";
	  public static final String fourHundredfour = "404";
	  
	  //For robots txt parsing
	  
	  public static final String robotUserAgent = "User-agent";
	  public static final String disallow = "Disallow:";
	  public static final String cis455UserAgent ="cis455crawler";
	  public static final String allow = "Allow:";
	  public static final String crawldelay = "Crawl-delay:";
	  
	  
	  //For url
	  public static final String http ="http"; 
	  public static final String https ="https";
	  
	  //For nodes
	  public static final String hashText = "#text";
	  public static final String seedCountText = "nexttimestartwith";

	  public static final String hashComment = "#comment";
	  public static final String href ="href";
	  public static String makeIfModifiedSinceHeader(long time)
	  {
			 SimpleDateFormat sdf = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss zzz");
			 sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
			 String fileLastUnmodified = ifModifiedSince+sdf.format(time);;
			 return fileLastUnmodified;

	  }
	  
	  public static String makeLocation(long time)
	  {
		  SimpleDateFormat sdf = new SimpleDateFormat("YYYY-MM-DD hh:mm:ss");
		  sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
		  String fileLastUnmodified  = sdf.format(time);
		  String s[] = fileLastUnmodified.split(" ");
		  fileLastUnmodified = s[0]+"T"+s[1];
		  return fileLastUnmodified;
	  }
	  public static String convertToSHAHash(String key)
		{
			MessageDigest md;
			 StringBuffer sb = new StringBuffer();
			 try
			 {
				md = MessageDigest.getInstance("SHA-1");
		        md.update(key.getBytes());
		        byte byteData[] = md.digest();
		        for (int i = 0; i < byteData.length; i++) 
		        {
		        	sb.append(Integer.toString((byteData[i] & 0xff) + 0x100, 16).substring(1));
		        }
			  } 
			 catch (NoSuchAlgorithmException e) 
			 {
				System.out.println("Exception while converting into SHA-1");
			 }
		        return sb.toString();

		}
	  
		public static String checksum(String s)
		{
			long value = 0;
			try{ 
				  String string = s;
				  byte buffer[] = string.getBytes();
				  ByteArrayInputStream bais = new ByteArrayInputStream(buffer);
				  CheckedInputStream cis = new CheckedInputStream(bais, new Adler32());
				  byte readBuffer[] = new byte[string.length()];
				  while (cis.read(readBuffer) >= 0)
				  {
					  value = cis.getChecksum().getValue();
				  }
				}
				  catch(Exception e)
				  {
					  //TODO LOG
				  //System.out.println("Exception has been caught" + e);
				  }
			return String.valueOf(value);
		}

}

