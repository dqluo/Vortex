package edu.upenn.cis455.mapreduce.worker;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import edu.upenn.cis455.mapreduce.AllStrings;
import edu.upenn.cis455.s3Interface.S3Implementation;

public class UploadForIndexServlet extends HttpServlet 
{
	private static final long serialVersionUID = 401488683032300340L;

	public void doPost(HttpServletRequest request, HttpServletResponse response) throws java.io.IOException
	{
		System.out.println("Here");
		Worker.me = Integer.valueOf(request.getParameter(AllStrings.myWorkerIDString));
		String accessKey = request.getParameter(AllStrings.accessKeyStringIndex);
		String secretkey = request.getParameter(AllStrings.secretKeyStringIndex);
		String bucketName = request.getParameter(AllStrings.bucketNameStringIndex);
		String localPathToUpload = request.getParameter(AllStrings.uploadlocalStringIndex);
		String inputFolderInS3 = request.getParameter(AllStrings.inputFolderinS3StringIndex);
		System.out.println("caling ikmpl");
		S3Implementation s3imp = new S3Implementation();

		if(inputFolderInS3.contains("input")) {
			s3imp.createInputTxtToIndexer(Worker.storagedir+"Input/store", Worker.storagedir+localPathToUpload, Worker.me);
			//	s3imp.createInputTxtToRanker("/Users/karthikalle/Desktop/Instance-Stores/Worker1/store", localPathToUpload, Worker.me);

			//	s3imp.createInputTxtToRanker("BDBStore", localPathToUpload, Worker.me);


			s3imp.pushToS3(accessKey, secretkey, bucketName, inputFolderInS3, Worker.storagedir+localPathToUpload, Worker.me);

		}
		else if(inputFolderInS3.contains("ouput"))
			s3imp.fetchFromS3Indexer("");
		else {
			s3imp.fetchFromS3ImagesIndexer("");
		}
	}
}

