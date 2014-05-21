package edu.upenn.cis455.mapreduce.master;

import java.io.BufferedReader;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import edu.upenn.cis455.mapreduce.WorkerContext;


public class WorkerResultServlet extends HttpServlet 
{

  static final long serialVlersionUID = 455555002;

  public void doPost(HttpServletRequest request, HttpServletResponse response) 
       throws java.io.IOException
  {
	 // System.out.println("Worker Result Servlet has been called");
	  StringBuffer invertedIndex = new StringBuffer();
	  
	  BufferedReader readerInvIndex = request.getReader();
	  String linesOfInvIndex;
	  linesOfInvIndex = readerInvIndex.readLine();
	  String wordAndData[] ;
	  String data ;
      wordAndData = linesOfInvIndex.split("\t");
	  String word = wordAndData[0].trim();
	  System.out.println("I should always come  "+word);
	  //System.out.println("This is the inverted index "+invertedIndex);
	  if(linesOfInvIndex != null && !linesOfInvIndex.equals("null"))
	  {
		  String [] wordAndData1 = linesOfInvIndex.split("\t");
		  data = wordAndData1[1].trim();
		  word = wordAndData1[0].trim();
		  Master.wordInvIndex.put(word, data);
		  Master.workerNumberRequestSent.put(WorkerContext.checkAndRedirectRange(word),false);
	  }
	  else
	  {
		  Master.wordInvIndex.put("nothingreturned", null);
		  Master.workerNumberRequestSent.put(WorkerContext.checkAndRedirectRange(word),false);
	  } 
		  
	  Master.wordInvIndexReceived.put("nothingreturned",true);
	  Master.noOfRequestsForInvertedIndexSent_loop --;
	  
	  

  }
}