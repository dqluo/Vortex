package edu.upenn.cis455.S3;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

public class S3Access {
	public  ArrayList<String> getDictionary (){
		AWSCredentials credentials=new BasicAWSCredentials("AKIAITQVLDKWRLCPIPGQ","SniVYM30wQpG5kwPutgD7VUifj2czRvnfIMUhXVm");
		 AmazonS3 s3 = new AmazonS3Client(credentials);
		 
			//Region usWest2 = Region.getRegion(Regions.US_WEST_2);
			//s3.setRegion(usWest2);
			String bucketName = "iwsindex";
	       // String key = "Lexicon.txt";
			String key = "lexlemma.txt";
	        S3Object object = s3.getObject(new GetObjectRequest(bucketName, key));
         //System.out.println("Content-Type: "  + object.getObjectMetadata().getContentType());
	        ArrayList<String> lex=null;
         try {
			
				 lex=displayTextInputStream(object.getObjectContent());
			
		} catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which means your request made it "
                    + "to Amazon S3, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with S3, "
                    + "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        } catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return lex;
	 
		
		
	}
	
	//reading lrxicon from S3 and storing in arrayList
	 private  ArrayList<String> displayTextInputStream(InputStream input) throws IOException {
		 String line="";
		 ArrayList<String> lex=new ArrayList<String>();
		// StringBuilder sb=new StringBuilder();
	        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
	        while ((line=reader.readLine())!=null) {
	        	lex.add(line.trim());
	         //sb.append(line);
	        // sb.append(System.lineSeparator());
	        	
	            
	        }
	       return lex;
	       
	    }

}
