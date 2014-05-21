package edu.upenn.cis455.mapreduce.master;


import javax.servlet.http.*;

import edu.upenn.cis455.mapreduce.AllStrings;

public class RankerServlet extends HttpServlet {


	private static final long serialVersionUID = 1L;

	public void doPost(HttpServletRequest request, HttpServletResponse response) throws java.io.IOException
	{
		String accesskey  	 = request.getParameter("rankaccesskey");
		String secretkey  	 = request.getParameter("ranksecretkey");
		String bucketname  	 = request.getParameter("rankbucketname");
		String touploadinlocal = request.getParameter("rankuploadlocal");
		String inputfolderins3 = request.getParameter("rankinputfolderins3");


		String postBody = AllStrings.accessKeyStringRank+"="+accesskey+"&"+
				AllStrings.secretKeyStringRank+"="+secretkey+"&"+
				AllStrings.bucketNameStringRank+"="+bucketname+"&"+
				AllStrings.uploadlocalStringRank+"="+touploadinlocal+"&"+
				AllStrings.inputFolderinS3StringRank+"="+inputfolderins3;
		System.out.println("here");
		Master.callUploadForRank(postBody);  
	}
}

