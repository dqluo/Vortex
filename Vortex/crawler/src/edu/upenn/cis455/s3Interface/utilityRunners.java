package edu.upenn.cis455.s3Interface;

import org.junit.Test;


public class utilityRunners {

	@Test
	/* To be done by the worker and the path is to it's local crawler store */
	 public void createInputTextForRankerS3() {
		S3Implementation s3imp = new S3Implementation();
		s3imp.createInputTxtToRanker("/Users/karthikalle/Desktop/Instance-Stores/Worker1/store","InputToRanker", 8);
	}
	 
	@Test
	/* Then call this to push the data into S3 */
	public void pushInputTxtToS3() {
		S3Implementation s3imp = new S3Implementation();
	//	s3imp.pushToS3("AKIAITQVLDKWRLCPIPGQ","SniVYM30wQpG5kwPutgD7VUifj2czRvnfIMUhXVm","iwsindex","indexinput"
		//		,"/Users/karthikalle/Desktop/Instance-Stores/Worker1/store",8);
		s3imp.pushToS3("AKIAITQVLDKWRLCPIPGQ", "SniVYM30wQpG5kwPutgD7VUifj2czRvnfIMUhXVm", "indexsonal", "indexinput", "InputToIndexer", 8);
	}
		

	@Test
	public void printResultsIntoTxtFile() {
		S3Implementation s3imp = new S3Implementation();
		s3imp.seeResults("/Users/karthikalle/Desktop/testresults");
	}
	
	@Test
	public void createInputTextIndexerForS3() {
		S3Implementation s3imp = new S3Implementation();
		//s3imp.createInputTxtToIndexer("BDBStore", 2);
		s3imp.createInputTxtToIndexer("/Users/karthikalle/Desktop/Instance-Stores/Worker1/store","InputToIndexer", 8);


	}
	
	@Test
	public void fetchFileFromS3Index() {
		S3Implementation s3imp = new S3Implementation();
		s3imp.fetchFromS3Indexer("");

	}
	
	@Test
	public void fetchFileFromS3Rank() {
		S3Implementation s3imp = new S3Implementation();
		s3imp.fetchFromS3Ranker("");

	}
	
}
