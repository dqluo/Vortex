package edu.upenn.cis455.mapreduce.master;

import java.io.PrintWriter;

import javax.servlet.http.*;

import edu.upenn.cis455.mapreduce.AllStrings;

public class IndexerServlet extends HttpServlet
{

  
  public void doPost(HttpServletRequest request, HttpServletResponse response) 
       throws java.io.IOException
  {
   
	  String accesskey  	 = request.getParameter("indexaccesskey");
	  String secretkey  	 = request.getParameter("indexsecretkey");
	  String bucketname  	 = request.getParameter("indexbucketname");
	  String touploadinlocal = request.getParameter("indexuploadlocal");
	  String inputfolderins3 = request.getParameter("indexinputfolderins3");
	  
	  String postBody = AllStrings.accessKeyStringIndex+"="+accesskey+"&"+
	                    AllStrings.secretKeyStringIndex+"="+secretkey+"&"+
			            AllStrings.bucketNameStringIndex+"="+bucketname+"&"+
			            AllStrings.uploadlocalStringIndex+"="+touploadinlocal+"&"+
	                    AllStrings.inputFolderinS3StringIndex+"="+inputfolderins3;
			        
	  
	Master.callUploadForIndex(postBody);  
    
    
  }
}
  
