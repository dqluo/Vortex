package edu.upenn.cis455.mapreduce.storage;

import java.io.File;
import java.io.IOException;

import org.apache.log4j.PropertyConfigurator;

import com.sleepycat.bind.tuple.StringBinding;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.OperationStatus;

public class IndexerImageStore 
{

    private static final int EXIT_FAILURE = 1; 
    private File envDir;

    Database ImageIndexer;
    //Table for storing URLID and checksum

    Environment crawlerStore;

     


    public IndexerImageStore(String filename) 
    {
    	
    	envDir = new File(filename);
    	if(!envDir.exists())
    		envDir.mkdir();
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
         
         ImageIndexer = crawlerStore.openDatabase(null,"ImageIndexer",dbConfig);
  
    }

   //Start of insert and retrieve from URLId and URL table
    
    public void insertIntoImageIndexer(String wordId,String index)
    {
    	boolean enteredCatch = false;
    	try
    	{
	         DatabaseEntry keyEntry = new DatabaseEntry();
	         DatabaseEntry dataEntry = new DatabaseEntry();         
	         StringBinding.stringToEntry(wordId, keyEntry); 
	         StringBinding.stringToEntry(index, dataEntry);
	         OperationStatus status = ImageIndexer.put(null, keyEntry, dataEntry);
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
    public String retrieveFromImageIndexer(String wordId)
    {
    	boolean enteredCatch = false;
    	try
    	{
	        /* retrieve the data */
	        DatabaseEntry keyEntry = new DatabaseEntry();
	        DatabaseEntry dataEntry = new DatabaseEntry();
	        StringBinding.stringToEntry(wordId, keyEntry);
	        ImageIndexer.get(null, keyEntry, dataEntry, null);
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
    	ImageIndexer.close();
    }
    
    public void closeEnvironment()
    {
    	crawlerStore.close();
    }   

    public static void main(String argv[]) throws IOException
    {
        DatabaseEntry keyEntry = new DatabaseEntry();
        DatabaseEntry dataEntry = new DatabaseEntry();
        IndexerImageStore cs = new IndexerImageStore("/home/cis455/worker/Rama/store");
        cs.openEnv();
        cs.openDBTables();
        Cursor cursor = cs.ImageIndexer.openCursor(null, null);
        int i=0;
        cs.insertIntoImageIndexer("hiiiii", "1");
        cs.insertIntoImageIndexer("byeeee", "2");
        System.out.println(cs.retrieveFromImageIndexer("hiiiii"));
        while (cursor.getNext(keyEntry, dataEntry,null) == OperationStatus.SUCCESS) 
        {    	
        	String key = StringBinding.entryToString(keyEntry);
        	String data = StringBinding.entryToString(dataEntry);
	            System.out.println("key=" + key + " data=" + data);
	            i++;
        }
        
       // System.out.println(cs.retrieveFromDocIdDocContentDB(cs.retrieveFromURLIdDocIdDB("fceba3b3adebbc3b7a484d1e6e734406ffe96574")));
        
        System.out.println("Count "+i);


        cursor.close();
        //System.out.println(cs.retrieveFromURLIdURL(AllStringsCrawler.convertToSHAHash("http://crawltest.cis.upenn.edu/")));
        cs.closeDBTables();
        cs.closeEnvironment();
        
    }
	
	
}
