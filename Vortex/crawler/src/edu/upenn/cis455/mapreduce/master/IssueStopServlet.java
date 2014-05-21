package edu.upenn.cis455.mapreduce.master;


import javax.servlet.http.*;


public class IssueStopServlet extends HttpServlet
{

  static final long serialVersionUID = 455555001;
  static boolean reduceCheck = false;
  public void doPost(HttpServletRequest request, HttpServletResponse response) 
       throws java.io.IOException
  {
	  
		  if(Master.isMapping)
		  {
			  Master.shouldpush = false;
			  Master.isStopped = true;
			  Master.issueStop();		  
			  
		  }		  
  }
	
  }

