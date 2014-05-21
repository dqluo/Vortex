package edu.upenn.cis455.mapreduce.worker;


import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import edu.upenn.cis455.mapreduce.AllStrings;
//import edu.upenn.cis455.s3Interface.S3Implementation;
import edu.upenn.cis455.s3Interface.S3Implementation;

@SuppressWarnings("serial")
public class UploadForRankServlet extends HttpServlet 
{

	public void doPost(HttpServletRequest request, HttpServletResponse response) throws java.io.IOException
	{

		System.out.println("Here");
		Worker.me = Integer.valueOf(request.getParameter(AllStrings.myWorkerIDString));
		String accessKey = request.getParameter(AllStrings.accessKeyStringRank);
		String secretkey = request.getParameter(AllStrings.secretKeyStringRank);
		String bucketName = request.getParameter(AllStrings.bucketNameStringRank);
		String localPathToUpload = request.getParameter(AllStrings.uploadlocalStringRank);
		String inputFolderInS3 = request.getParameter(AllStrings.inputFolderinS3StringRank);
		System.out.println("caling impl");
		S3Implementation s3imp = new S3Implementation();

	/*	s3imp.createInputTxtToRanker(Worker.storagedir+"Input/store", Worker.storagedir+localPathToUpload, Worker.me);
	//	s3imp.createInputTxtToRanker("/Users/karthikalle/Desktop/Instance-Stores/Worker1/store", localPathToUpload, Worker.me);
		
	//	s3imp.createInputTxtToRanker("BDBStore", localPathToUpload, Worker.me);

		
		s3imp.pushToS3(accessKey, secretkey, bucketName, inputFolderInS3, Worker.storagedir+localPathToUpload, Worker.me);
	*/
		s3imp.fetchFromS3Ranker("");
	}
}



