package edu.upenn.cis455.s3Interface;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;

import com.sleepycat.bind.tuple.StringBinding;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.OperationStatus;
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import edu.upenn.cis455.mapreduce.WorkerContext;
import edu.upenn.cis455.mapreduce.master.Master;
import edu.upenn.cis455.mapreduce.storage.CrawlerStore;
import edu.upenn.cis455.mapreduce.storage.IndexerImageStore;
import edu.upenn.cis455.mapreduce.storage.IndexerStore;
import edu.upenn.cis455.mapreduce.storage.RankerStore;
import edu.upenn.cis455.mapreduce.worker.AllStringsCrawler;
import edu.upenn.cis455.mapreduce.worker.Worker;

public class S3Implementation {


	public void seeResults(String path) {
		File f = new File(path);
		if(!f.exists()) {
			System.out.println("Directory does not exist");
		}
		CrawlerStore cs = new CrawlerStore("/Users/karthikalle/Desktop/Instance-Stores/Worker1/store");
		cs.openEnv();
		cs.openDBTables();
		try {
			File resultFile = new File("/Users/karthikalle/Desktop/testresults/OrderedResults.txt");
			PrintWriter pw = null;
			pw = new PrintWriter(resultFile);
			HashMap<Float, String> reverseRanks = new HashMap<>();
			Float totalRank = (float)0.0;

			for(File resultFiles: f.listFiles()) {
				BufferedReader br;

				FileReader fr = new FileReader(resultFiles);
				br = new BufferedReader(fr);
				//	System.out.println(resultFiles);
				String str = "";
				while((str=br.readLine())!=null) {
					String pageAndChildren[] = str.split("\t");
					if(pageAndChildren.length>=2){
						String pageRanks[] = pageAndChildren[0].split(":");
						//String url = cs.retrieveFromURLIdURL(pageRanks[0]);
						String url = pageRanks[0];
						Float rank = Float.parseFloat(pageRanks[1]);
						if(url!=null) {
							//	System.out.println(url+" "+rank);
							totalRank += (Float)rank;

							if(reverseRanks.containsKey(rank)) 
								reverseRanks.put(rank, reverseRanks.get(rank)+";"+url);
							else 
								reverseRanks.put(rank, url+";");
						}
					}

				}
				fr.close();
				br.close();
			}
			Object[] keys = reverseRanks.keySet().toArray();
			Arrays.sort(keys, Collections.reverseOrder());


			for(Object key : keys) {
				for(String each: reverseRanks.get(key).split(";")){

					if(each.length()!=0)
						pw.write(URLDecoder.decode(each,"UTF-8")+": "+key+"\n");
				}
			}

			System.out.println("total rank: "+totalRank);
			pw.close();

		}
		catch (FileNotFoundException e1) {
			e1.printStackTrace();
		}catch (IOException e) {
			e.printStackTrace();
		}


		cs.closeDBTables();
		cs.closeEnvironment();

	}

	/* TODO: work on this */
	public void fetchFromS3Ranker(String s3FilePath) {
		ArrayList<String> keys;
		String bucketName = "iwsranking";

		//	String key = "input/InputToS3"+1+".txt";

		AWSCredentials credentials = new BasicAWSCredentials("AKIAI6MB5GHKO6CRTEIQ", "iQ3QglXxzYwBC1uNWCBuj+6feU0VEeGlQgGVR2aB");
		AmazonS3 s3Client = new AmazonS3Client();
		s3Client.setEndpoint("s3.amazonaws.com");	
		s3Client = new AmazonS3Client(credentials);

		Worker.me = Master.getWorkerNumber(Worker.workerport);
		RankerStore rankerStore = new RankerStore("/home/ec2-user/worker"+Worker.me+"/Input/RankerStore");
		rankerStore.openEnv();
		rankerStore.openDBTables();
		Worker.noOfWorkers = 10;

		keys = listObjectsInBucketRanking(bucketName);
		try {

			File f = new File("TestResults.txt");
			if(f.exists())
				f.delete();
			f.createNewFile();
			PrintWriter pw;
			pw = new PrintWriter(f);


			for(String key: keys) {
				System.out.println("Downloading an object");
				S3Object s3object = s3Client.getObject(new GetObjectRequest(bucketName, key));
				System.out.println("Content-Type: " + s3object.getObjectMetadata().getContentType());
				//displayTextInputStream(s3object.getObjectContent());
				parseInputOfS3Ranking(s3object.getObjectContent(), pw, rankerStore);

			}
			f.delete();
			pw.close();
		} catch (AmazonServiceException ase) {
			ase.printStackTrace();
		} catch (AmazonClientException ace) {
			ace.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		finally {
			System.out.println("Pulling completed");
			rankerStore.closeDBTables();
			rankerStore.closeEnvironment();
		}

	}

	public void fetchFromS3Indexer(String s3FilePath)
	{
		ArrayList<String> keys;
		String bucketName = "indexsonal";

		//	String key = "input/InputToS3"+1+".txt";

		AWSCredentials credentials = new BasicAWSCredentials("AKIAITQVLDKWRLCPIPGQ", "SniVYM30wQpG5kwPutgD7VUifj2czRvnfIMUhXVm");
		AmazonS3 s3Client = new AmazonS3Client();
		s3Client.setEndpoint("s3.amazonaws.com");	
		s3Client = new AmazonS3Client(credentials);

		Worker.me = Master.getWorkerNumber(Worker.workerport);
		IndexerStore indexStore = new IndexerStore("/home/ec2-user/worker"+Worker.me+"/Input/IndexStore");
		//IndexerStore indexStore = new IndexerStore("IndexStore");

		indexStore.openEnv();
		indexStore.openDBTables();
		Worker.noOfWorkers = 10;

		keys = listObjectsInBucketIndexing(bucketName, "ouput");
		try {
			File f = new File("IndexResuts.txt");
			if(f.exists())
				f.delete();
			f.createNewFile();
			PrintWriter pw;
			pw = new PrintWriter(f);


			for(String key: keys) {
				System.out.println("Downloading an object");
				S3Object s3object = s3Client.getObject(new GetObjectRequest(bucketName, key));
				System.out.println("Content-Type: " + s3object.getObjectMetadata().getContentType());
				//displayTextInputStream(s3object.getObjectContent());
				parseInputOfS3Indexing(s3object.getObjectContent(), pw, indexStore);

			}
			f.delete();
			pw.close();
		} catch (AmazonServiceException ase) {
			ase.printStackTrace();
		} catch (AmazonClientException ace) {
			ace.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		finally {
			System.out.println("Pulling completed");
			indexStore.closeDBTables();
			indexStore.closeEnvironment();
		}

	}

	public void fetchFromS3ImagesIndexer(String s3FilePath)
	{
		ArrayList<String> keys;

		String bucketName = "imageindex";
		String folderInBucket = "output";
		//	String key = "input/InputToS3"+1+".txt";

		AWSCredentials credentials = new BasicAWSCredentials("AKIAJH6WPG2AIG4JPJNQ", "brpgeQvX6jptcnrBcl5a0DeUg2Y+BrObCg5qRUVd");
		AmazonS3 s3Client = new AmazonS3Client();
		s3Client.setEndpoint("s3.amazonaws.com");	
		s3Client = new AmazonS3Client(credentials);

		Worker.me = Master.getWorkerNumber(Worker.workerport);
		IndexerImageStore indexStore = new IndexerImageStore("/home/ec2-user/worker"+Worker.me+"/Input/ImageStore");
	//	IndexerStore indexStore = new IndexerStore("IndexStore");

		indexStore.openEnv();
		indexStore.openDBTables();
		Worker.noOfWorkers = 10;

		keys = listObjectsInBucketIndexing(bucketName, folderInBucket);
		try {
			File f = new File("IndexResuts.txt");
			if(f.exists())
				f.delete();
			f.createNewFile();
			PrintWriter pw;
			pw = new PrintWriter(f);


			for(String key: keys) {
				System.out.println("Downloading an object");
				S3Object s3object = s3Client.getObject(new GetObjectRequest(bucketName, key));
				System.out.println("Content-Type: " + s3object.getObjectMetadata().getContentType());
				//displayTextInputStream(s3object.getObjectContent());
				parseInputOfS3ImageIndexing(s3object.getObjectContent(), pw, indexStore);

			}
			f.delete();
			pw.close();
		} catch (AmazonServiceException ase) {
			ase.printStackTrace();
		} catch (AmazonClientException ace) {
			ace.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		finally {
			System.out.println("Pulling completed");
			indexStore.closeDBTables();
			indexStore.closeEnvironment();
		}

	}
	public static void displayTextInputStream(InputStream input)
			throws IOException {
		// Read one text line at a time and display.
		BufferedReader reader = new BufferedReader(new InputStreamReader(input));
		while (true) {
			String line = reader.readLine();
			if (line == null) break;

			System.out.println(line);
		}
		System.out.println();
	}

	/* Check for each wordID or URLid, if it belongs to me and store in bdb */
	public static void parseInputOfS3Ranking(InputStream input, PrintWriter pw, RankerStore rankerStore)
			throws IOException {
		// Read one text line at a time and display.
		BufferedReader reader = new BufferedReader(new InputStreamReader(input));
		while (true) {
			String line = reader.readLine();
			//if(key within my range) -> insert into BDB
			if (line == null) 
				break;
			String key = line.split("\t")[0];

			String urlIdandRank[] = key.split(":");
			String url = urlIdandRank[0];
			String rank = urlIdandRank[1];
			//		System.out.println(url+"-"+rank);
			rankerStore.insertIntoRanker(url,rank);
			System.out.println(rankerStore.retrieveFromRanker(url));
			//	pw.write(url+"-"+rank);
			//pw.write("\n");
			//System.out.println(line);
		}
	}

	/* Check for each wordID or URLid, if it belongs to me and store in bdb */
	public static void parseInputOfS3Indexing(InputStream input, PrintWriter pw, IndexerStore indexStore)
			throws IOException {
		// Read one text line at a time and display.

		BufferedReader reader = new BufferedReader(new InputStreamReader(input));
		while (true) {
			String line = reader.readLine();
			//if(key within my range) -> insert into BDB
			if (line == null) 
				break;
			String key[] = line.split("\t");

			String word = key[0];
			String invertedIndex = key[1];
			//System.out.println(word + "\t" + invertedIndex);

			int hash = WorkerContext.checkAndRedirectRange(word);
			if(hash==Worker.me) {
				indexStore.insertIntoIndexer(word, invertedIndex);
				System.out.println(word);
			}

			//pw.write(word+"\t"+invertedIndex);
			//pw.write("\n");
			//System.out.println(line);
		}
	}

	public static void parseInputOfS3ImageIndexing(InputStream input, PrintWriter pw, IndexerImageStore indexStore)
			throws IOException {
		// Read one text line at a time and display.

		BufferedReader reader = new BufferedReader(new InputStreamReader(input));
		while (true) {
			String line = reader.readLine();
			//if(key within my range) -> insert into BDB
			if (line == null) 
				break;
			String key[] = line.split("\t");

			String word = key[0];
			String invertedIndex = key[1];
			//System.out.println(word + "\t" + invertedIndex);

			int hash = WorkerContext.checkAndRedirectRange(word);
			if(hash==Worker.me) {
				indexStore.insertIntoImageIndexer(word, invertedIndex);
				System.out.println(word);
			}

			//pw.write(word+"\t"+invertedIndex);
			//pw.write("\n");
			//System.out.println(line);
		}
	}

	public static ArrayList<String> listObjectsInBucketRanking(String bucketName) {

		ArrayList<String> keys = new ArrayList<String>();
		AWSCredentials credentials = new BasicAWSCredentials("AKIAI6MB5GHKO6CRTEIQ", "iQ3QglXxzYwBC1uNWCBuj+6feU0VEeGlQgGVR2aB");
		AmazonS3 s3Client = new AmazonS3Client();
		s3Client.setEndpoint("s3.amazonaws.com");	
		s3Client = new AmazonS3Client(credentials);

		try {
			System.out.println("Listing objects");

			ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
			.withBucketName(bucketName).withPrefix("outputs/primaryOutput1399518871949");

			ObjectListing objectListing;            
			do {
				objectListing = s3Client.listObjects(listObjectsRequest);
				for (S3ObjectSummary objectSummary : 
					objectListing.getObjectSummaries()) {
					keys.add(objectSummary.getKey());
					System.out.println(" - " + objectSummary.getKey() + "  " +
							"(size = " + objectSummary.getSize() + 
							")");
				}
				listObjectsRequest.setMarker(objectListing.getNextMarker());
			} while (objectListing.isTruncated());
		} catch (AmazonServiceException ase) {
			ase.printStackTrace();
		} catch (AmazonClientException ace) {
			ace.printStackTrace();
		}
		return keys;
	}

	public static ArrayList<String> listObjectsInBucketIndexing(String bucketName, String folderName) {

		ArrayList<String> keys = new ArrayList<String>();
		String accessKey = "";
		String secretKey = "";
		if(!folderName.contains("ouput")) {
			accessKey = "AKIAJH6WPG2AIG4JPJNQ";
			secretKey = "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";
		}
		else {
			accessKey = "AKIAITQVLDKWRLCPIPGQ";
			secretKey = "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";
		}
		
		AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
		AmazonS3 s3Client = new AmazonS3Client();
		s3Client.setEndpoint("s3.amazonaws.com");	
		s3Client = new AmazonS3Client(credentials);

		try {
			System.out.println("Listing objects in "+bucketName);

			ListObjectsRequest listObjectsRequest = new ListObjectsRequest().withBucketName(bucketName).withPrefix(folderName);
			System.out.println(listObjectsRequest.getPrefix());
			ObjectListing objectListing;            
			do {
				objectListing = s3Client.listObjects(listObjectsRequest);
				for (S3ObjectSummary objectSummary : 
					objectListing.getObjectSummaries()) {
					keys.add(objectSummary.getKey());
					System.out.println(" - " + objectSummary.getKey() + "  " +
							"(size = " + objectSummary.getSize() + 
							")");
				}
				listObjectsRequest.setMarker(objectListing.getNextMarker());
			} while (objectListing.isTruncated());
		} catch (AmazonServiceException ase) {
			ase.printStackTrace();
		} catch (AmazonClientException ace) {
			ace.printStackTrace();
		}
		return keys;
	}

	public void pushToS3(String accesskey, String secretAccessKey, String bucketName, String folderInBucket, String inputDirForRanker, int workerID) {


		//	AWSCredentials credentials = new BasicAWSCredentials("AKIAI6MB5GHKO6CRTEIQ", "iQ3QglXxzYwBC1uNWCBuj+6feU0VEeGlQgGVR2aB");
		//		if(secretAccessKey.contains("+"))
		//			secretAccessKey = secretAccessKey.replace(" ","+");
		/*if(folderInBucket.equals("input")){
			accesskey = "AKIAITQVLDKWRLCPIPGQ";
			secretAccessKey = "SniVYM30wQpG5kwPutgD7VUifj2czRvnfIMUhXVm";
		}
		else {
			accesskey = "AKIAI6MB5GHKO6CRTEIQ";
			secretAccessKey = "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";

		}*/
		accesskey = "AKIAJH6WPG2AIG4JPJNQ";
		secretAccessKey = "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";
		bucketName = "imageindex";
		folderInBucket = "imageinput";
		AWSCredentials credentials = new BasicAWSCredentials(accesskey, secretAccessKey);

		System.out.println(accesskey+"\t"+ secretAccessKey);
		ClientConfiguration clientConfig = new ClientConfiguration();
		clientConfig.setProtocol(Protocol.HTTP);		
		AmazonS3 s3Client = new AmazonS3Client(clientConfig);
		s3Client.setEndpoint("s3.amazonaws.com");		
		//	String existingBucketName = "iwsranking";
		//String amazonFileUploadLocationOriginal = existingBucketName+"/RankerInput";
		String amazonFileUploadLocationOriginal = bucketName+"/"+folderInBucket;
		System.out.println(amazonFileUploadLocationOriginal);
		int fileNumber = 0;

		try {
			s3Client = new AmazonS3Client(credentials);
			File f = new File(inputDirForRanker);
			for(File file: f.listFiles()) {
				fileNumber++;
				String keyName = "InputToS3-"+workerID+"-"+fileNumber+".txt";
				//String keyName = file.getName();
				System.out.println(keyName);
				//file = new File("/Users/karthikalle/Desktop/Vortex/VortexSearch/Project/InputToRanker/InputToS3.txt");
				//FileInputStream stream = new FileInputStream("/Users/karthikalle/Desktop/Vortex/VortexSearch/Project/InputToRanker/InputToS3.txt");
				FileInputStream stream = new FileInputStream(file);
				ObjectMetadata objectMetadata = new ObjectMetadata();
				objectMetadata.setContentType("text/plain");
				objectMetadata.setContentLength(file.length());
				PutObjectRequest putObjectRequest = new PutObjectRequest(amazonFileUploadLocationOriginal, keyName, stream, objectMetadata);
				putObjectRequest.setInputStream(stream);
				PutObjectResult result = s3Client.putObject(putObjectRequest);
				System.out.println("Etag:" + result.getETag() + "-->" + result);
				stream.close();
			}
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}


	public void createInputTxtToRanker(String storePath, String inputDirectory, int workerID) {
		CrawlerStore crawlStore = new CrawlerStore(storePath);
		crawlStore.openEnv();
		crawlStore.openDBTables();

		DatabaseEntry keyEntry = new DatabaseEntry();
		DatabaseEntry dataEntry = new DatabaseEntry();

		Cursor cursor = crawlStore.URLIdURLto.openCursor(null, null);
		int i=0;

		int iterations = 0;
		int count = 0;
		File f = null;
		//inputdir
		//f = new File("InputToRanker");
		f = new File(inputDirectory);
		if(f.exists()) {
			deleteDir(f);
		}
		f.mkdir();
		PrintWriter pw = null;
		while (cursor.getNext(keyEntry, dataEntry,null) == OperationStatus.SUCCESS) {

			if(count == 1000 || count == 0) {
				if(count!=0)
					pw.close();
				count = 0;
				iterations++;

				f = new File(inputDirectory+"/InputToS3-"+workerID+"-"+iterations+".txt");

				if(f.exists()) {
					f.delete();
					f.mkdir();
				}
				try {
					pw = new PrintWriter(f);
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				}
			}
			String key = StringBinding.entryToString(keyEntry);
			String data = StringBinding.entryToString(dataEntry);

			StringBuffer lineEntry = new StringBuffer();
			boolean hasChildren = false;
			if(data.equals("")||data.equals(null))
				System.out.println("Key="+crawlStore.retrieveFromURLIdURL(key));
			else {
				//pw.write(key+"\t");
				lineEntry.append(key+"\t");
				String hostNameOfFrom = getHostFromURL(crawlStore, key);
				if(hostNameOfFrom==null)
					continue;
				for(String eachURL: data.split(";")) {
					String hostNameOfTo = getHostFromURL(crawlStore, eachURL);
					//System.out.println(hostNameOfTo+" "+hostNameOfFrom);
					if(hostNameOfTo == null)
						continue;
					if(!hostNameOfFrom.equals(hostNameOfTo)){
						//pw.write(eachURL+";");
						hasChildren = true;
						//System.out.println(crawlStore.retrieveFromURLIdURL(key)+"\t"+crawlStore.retrieveFromURLIdURL(eachURL));
						//	pw.write(crawlStore.retrieveFromURLIdURL(key)+"\t"+crawlStore.retrieveFromURLIdURL(eachURL));
						//	pw.write("\n");
						lineEntry.append(eachURL+";");
					}
				}
				if(hasChildren) {
					//	System.out.println(lineEntry.toString());
					pw.write(lineEntry.toString());
					pw.write("\n");
					count++;
					i++;
				}
			}
			if(count == 0)
				f.delete();
		}
		cursor.close();
		System.out.println("\nCount"+i);
		crawlStore.closeDBTables();
		crawlStore.closeEnvironment();

	}

	//public String getHostFromURL(CrawlerStore cs, String url) {
	public static String getHostFromURL(CrawlerStore cs, String u){
		String url = cs.retrieveFromURLIdURL(u);
		if(url == null || url.length() == 0)
			return null;
		if(url.contains("!!!!!@@@@@@@@!!!!!!@@@@@@!!!!!@@@!!!"))
			return null;

		int doubleslash = url.indexOf("//");
		if(doubleslash == -1)
			doubleslash = 0;
		else
			doubleslash += 2;

		int end = url.indexOf('/', doubleslash);
		end = end >= 0 ? end : url.length();

		int port = url.indexOf(':', doubleslash);
		end = (port > 0 && port < end) ? port : end;

		// System.out.println(url+"\t"+url.substring(doubleslash, end));
		return url.substring(doubleslash, end);
	}
	/*	String urlFrom = cs.retrieveFromURLIdURL(url);
		if(urlFrom==null)
			return null;
		if(urlFrom.contains("!!!!!@@@@@@@@!!!!!!@@@@@@!!!!!@@@!!!"))
			return null;
		urlFrom = urlFrom.replace(" ", "%20");

		URI uri = null;
		try {
			uri = new URI(urlFrom);
		} catch (URISyntaxException e) {
			System.out.println("url"+urlFrom);
			e.printStackTrace();
		}

		return uri.getHost();
	}*/
	private void deleteDir(File f) {
		if(f.isDirectory()) {
			for(File files: f.listFiles())
				deleteDir(files);
		}
		else {
			f.delete();
		}
	}

	public void createInputTxtToIndexer(String storePath, String inputDirectory, int workerID) 
	{
		CrawlerStore crawlStore = new CrawlerStore(storePath);
		crawlStore.openEnv();
		crawlStore.openDBTables();

		DatabaseEntry keyEntry = new DatabaseEntry();
		DatabaseEntry dataEntry = new DatabaseEntry();

		Cursor cursor = crawlStore.URLIdURL.openCursor(null, null);
		int i=0;

		int count = 0;
		File f = null;
		//	f = new File("InputToIndexer");
		f = new File(inputDirectory);
		if(f.exists()) {
			deleteDir(f);
		}

		f.mkdir();
		PrintWriter pw = null;
		int iterations = 0;

		while (cursor.getNext(keyEntry, dataEntry,null) == OperationStatus.SUCCESS) 
		{

			if(count == 1000 || count == 0) {
				if(count!=0)
					pw.close();
				count = 0;

				iterations++;
				f = new File(inputDirectory+"/InputToS3-"+workerID+"-"+iterations+".txt");
				if(f.exists()) {
					try {
						f.delete();
						f.createNewFile();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				try {
					pw = new PrintWriter(f);
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				}
			}
			String key = StringBinding.entryToString(keyEntry);
			//String data = StringBinding.entryToString(dataEntry);
			String urlOfImage = StringBinding.entryToString(dataEntry);
			//System.out.println("key=" + retrieveFromURLIdURL(key) + " data=" + data);
			if(urlOfImage.equals(null) )
				System.out.println("Key="+key);
			else 
			{
				if(key.equals("0"))
					continue;

				String content = crawlStore.retrieveFromURLIdContentImgDB(key);
				//	String urlid = crawlStore.retrieveFromURLIdURL(key);
				//String url = crawlStore.retrieveFromURLIdURL(urlid);
				//	System.out.println("URLId "+urlid);
				//	System.out.println("docid: "+key);
				/*if(url == null) {
					if(urlid == null)
						continue;

					String urlids[] = urlid.split(";");
					url = urlids[0];*/
				/*
					for(String urlsithink: urlids)
						System.out.println("think url is :"+crawlStore.retrieveFromURLIdURL(urlsithink));
				 */
				//continue;
				//	}


				if(!urlOfImage.contains("!!!!!@@@@@@@@!!!!!!@@@@@@!!!!!@@@!!!")){
					//pw.write(key+"\t"+urlOfImage+"<*&SEPARATOR&*>"+content+"\n");
					//System.out.println("URL "+urlOfImage);
					count++;
					i++;
				}
				else {
					//urlOfImage.split("!!!!!@@@@@@@@!!!!!!@@@@@@!!!!!@@@!!!")

					String urlAndContent[] = urlOfImage.split("!!!!!@@@@@@@@!!!!!!@@@@@@!!!!!@@@!!!");
					String url = urlAndContent[0];
					if(urlAndContent.length>1) {
					//	System.out.println(urlOfImage.split("!!!!!@@@@@@@@!!!!!!@@@@@@!!!!!@@@!!!")[0]);
						String urlhash = AllStringsCrawler.convertToSHAHash(url);
						String imageContent = urlAndContent[1];
						pw.write(urlhash+"\t"+url+"<*&SEPARATOR&*>"+imageContent+"\n");
						System.out.println(urlhash+"\t"+url+"<*&SEPARATOR&*>"+imageContent+"\n");
					}
				}
			}
			//	i++;
			if(count == 0)
				f.delete();
		}
		cursor.close();
		System.out.println("\nCount"+i);
	}
	public void createOutputTxt() 
	{

	}


}
