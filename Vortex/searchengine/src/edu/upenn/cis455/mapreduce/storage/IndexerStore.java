/* This file creates all the storage as key value pairs
 * All key values are encoded in strings and not objects since it would be easy in case
 * Elastic map reduce is used
 * checksum of the document is used as document ID
 * checksum and document ID are used synonymously in the document
 */

package edu.upenn.cis455.mapreduce.storage;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import com.sleepycat.bind.tuple.LongBinding;
import com.sleepycat.bind.tuple.StringBinding;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.OperationStatus;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import edu.upenn.cis455.mapreduce.WorkerContext;
import edu.upenn.cis455.mapreduce.worker.Worker;


public class IndexerStore 
{
    private static final int EXIT_FAILURE = 1; 
    private File envDir;

    Database Indexer;
    //Table for storing URLID and checksum

    Environment crawlerStore;



    public IndexerStore(String filename) 
    {
    	
    	envDir = new File(filename);
        PropertyConfigurator.configure("Log/log4j.properties");

    }
    public static void usage() {
        System.out.println("usage: java " +
                           "je.BindingExample " +
                           "<envHomeDirectory> " +
                           "<insert|retrieve> <numRecords> [offset]");
        System.exit(EXIT_FAILURE);
    }

    public void openEnv()
    {
    	EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        envConfig.setTransactionalVoid(false);
        envConfig.setLockingVoid(false);
        crawlerStore = new Environment(envDir, envConfig);
    }
    //Open all tables and keep during the crawling
    public void openDBTables()
    {
        
         DatabaseConfig dbConfig = new DatabaseConfig();
         dbConfig .setAllowCreate(true);
         
         //Normal document data
         
         Indexer = crawlerStore.openDatabase(null,"Indexer",dbConfig);
  
    }

   //Start of insert and retrieve from URLId and URL table
    
    public void insertIntoIndexer(String wordId,String index)
    {
    	boolean enteredCatch = false;
    	try
    	{
	         DatabaseEntry keyEntry = new DatabaseEntry();
	         DatabaseEntry dataEntry = new DatabaseEntry();         
	         StringBinding.stringToEntry(wordId, keyEntry); 
	         StringBinding.stringToEntry(index, dataEntry);
	         OperationStatus status = Indexer.put(null, keyEntry, dataEntry);
	         if (status != OperationStatus.SUCCESS) 
	         {
	                 throw new RuntimeException("Data insertion got status " +
	                                            status);
	         }
    	}
    	catch(Exception e)
    	{
    		enteredCatch = true;
    		
    	}
    	finally
    	{
    		if(enteredCatch)
    		{
	    		closeDBTables();
	    		closeEnvironment();
    		}
    	}
    }
    public String retrieveFromIndexer(String wordId)
    {
    	boolean enteredCatch = false;
    	try
    	{
	        /* retrieve the data */
	        DatabaseEntry keyEntry = new DatabaseEntry();
	        DatabaseEntry dataEntry = new DatabaseEntry();
	        StringBinding.stringToEntry(wordId, keyEntry);
	        Indexer.get(null, keyEntry, dataEntry, null);
	    	if(dataEntry.getData() != null)
	    	{
	    		String data =StringBinding.entryToString(dataEntry);
	    		return data;
	    	}
	    	else 
	    		return null;
    	}
    	catch(Exception e)
    	{
    		enteredCatch = true;
    		
    	}
    	finally
    	{
    		if(enteredCatch)
    		{
	    		closeDBTables();
	    		closeEnvironment();
    		}
    	}
    	return null;
    }
    

    
    public void closeDBTables()
    {
    	Indexer.close();
    }
    
    public void closeEnvironment()
    {
    	crawlerStore.close();
    }   

    public static void main(String argv[]) throws IOException
    {
        IndexerStore cs1 = new IndexerStore("/home/cis455/worker/Rama/IndexStore");
        IndexerStore cs2 = new IndexerStore("/home/cis455/worker1/Rama/IndexStore");
        IndexerStore cs3 = new IndexerStore("/home/cis455/worker2/Rama/IndexStore");
        //cs1.openEnv();
        //cs1.openDBTables();
        //cs2.openEnv();
        //cs2.openDBTables();
        cs3.openEnv();
        cs3.openDBTables();

        int i = 0;
        int count = 0;
        Worker.noOfWorkers = 3;
        /*for(i =0;i<=13;i++)
        {
    		File seedFileLoad = new File("/home/cis455/LEMMA/"+i+".txt");
    		System.out.println("From seed reader "+seedFileLoad.getAbsolutePath());

    			if(seedFileLoad.exists())
    			{
    	
    				BufferedReader seedFilerReaderLoad = new BufferedReader(new FileReader(seedFileLoad));
    				String line;
    				while((line = seedFilerReaderLoad.readLine())!= null)
    				{
    					count++;
    					String value[] = line.split("\t");	
    					int hash = WorkerContext.checkAndRedirectRange(value[0]);
    					
    					switch(hash)
    					{
	    					case 1: cs1.insertIntoIndexer(value[0], value[1]);break;
	    					case 2:cs2.insertIntoIndexer(value[0], value[1]);break;
	    					case 3:cs3.insertIntoIndexer(value[0], value[1]);break;		
    					}
    					
    				}
    				seedFilerReaderLoad.close();
    			}
    			
        }*/
        DatabaseEntry keyEntry = new DatabaseEntry();
        DatabaseEntry dataEntry = new DatabaseEntry();
        Cursor cursor = cs3.Indexer.openCursor(null, null);
       // int i=0;
        while (cursor.getNext(keyEntry, dataEntry,null) == OperationStatus.SUCCESS) 
        {    	
        	String key = StringBinding.entryToString(keyEntry);
        	String data = StringBinding.entryToString(dataEntry);
        	//if(data.contains(".pdf"))
        	{
	           // System.out.println("key=" + key + " data=" + data);
	            count++;
        	}
        }
        
        
        
        System.out.println("Count "+ count);
        //System.out.println(cs.retrieveFromURLIdURL(AllStringsCrawler.convertToSHAHash("http://crawltest.cis.upenn.edu/")));
       cursor.close();
       // cs1.closeDBTables();
        //cs1.closeEnvironment();
       // cs2.closeDBTables();
        //cs2.closeEnvironment();
       cs3.closeDBTables();
       cs3.closeEnvironment();
        
    }
    
        
}
