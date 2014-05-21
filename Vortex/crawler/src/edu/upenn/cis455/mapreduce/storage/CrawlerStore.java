/* This file creates all the storage as key value pairs
 * All key values are encoded in strings and not objects since it would be easy in case
 * Elastic map reduce is used
 * checksum of the document is used as document ID
 * checksum and document ID are used synonymously in the document
 */

package edu.upenn.cis455.mapreduce.storage;
/* This file creates all the storage as key value pairs
 * All key values are encoded in strings and not objects since it would be easy in case
 * Elastic map reduce is used
 * checksum of the document is used as document ID
 * checksum and document ID are used synonymously in the document
 */
import java.io.File;
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


public class CrawlerStore 
{
    private static final int EXIT_FAILURE = 1; 
    private File envDir;

    //Final project crawling tables
    //The first one mentioned below is always the key
    //Table for storing URLID and URL'2
    public Database URLIdURL;
    //Table for storing URLID and checksum
    Database URLIdDocId;
    //Table for storing URLID and document content
    public Database DocIdDocContent;
    //Table for storing URLID and number of words in the document
   // Database URLIdNoOfWords;
    //Table for storing URLID and content type, future purposes
    //Database URLIdContentType;
    //Table for storing URLID and when last crawled
    Database HostIdWhenLastCrawled;
    //Table for storing outgoing links from a url which are separated by ;
    public Database URLIdURLto;
    //Table for storing DocID and URLID if more than one url have same doc then they seperated by ;
    Database DocIdURLId;
    //Table for storing the host name and crawldelay
    Database HostIdCrawlDelay;
    Database HostIdDisallow;
    Database HostIdAllow;
    
    //Databases of images
    public Database URLIdURLImg;
    Database URLIdContentImg;
    Environment crawlerStore;

    static Logger logger = Logger.getLogger(CrawlerStore.class);


    public CrawlerStore(String filename) 
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
        logger.info("Environment opened.");
    }
    //Open all tables and keep during the crawling
    public void openDBTables()
    {
        
         DatabaseConfig dbConfig = new DatabaseConfig();
         dbConfig .setAllowCreate(true);
         
         //Normal document data
         
         URLIdURL   		  = crawlerStore.openDatabase(null,"URLIdURL",dbConfig);
         URLIdDocId 		  = crawlerStore.openDatabase(null,"URLIDDocumentID",dbConfig);
         DocIdDocContent 	  = crawlerStore.openDatabase(null,"URLIdDocumentContent",dbConfig);
         HostIdWhenLastCrawled = crawlerStore.openDatabase(null,"URLIDWhenLastCrawled",dbConfig);
         URLIdURLto 		  = crawlerStore.openDatabase(null,"URLIdURLTo",dbConfig);
         DocIdURLId 		  = crawlerStore.openDatabase(null,"DocIdURLId",dbConfig);
         HostIdCrawlDelay		  = crawlerStore.openDatabase(null,"HostIdCrawlDelay",dbConfig);
         HostIdDisallow		  = crawlerStore.openDatabase(null,"URLIdDisallow",dbConfig);
         HostIdAllow			  = crawlerStore.openDatabase(null,"IIdAllow",dbConfig);
         
         //Image Database
         URLIdURLImg  	  		= crawlerStore.openDatabase(null, "URLIdURLImg", dbConfig);
         URLIdContentImg  		= crawlerStore.openDatabase(null, "URLIdURLContentImg", dbConfig);
         
         logger.info("All tables opened.");
    }

   //Start of insert and retrieve from URLId and URL table
    
    public void insertIntoURLIdURL(String URLId,String URL)
    {
    	boolean enteredCatch = false;
    	try
    	{
	         DatabaseEntry keyEntry = new DatabaseEntry();
	         DatabaseEntry dataEntry = new DatabaseEntry();         
	         StringBinding.stringToEntry(URLId, keyEntry); 
	         StringBinding.stringToEntry(URL, dataEntry);
	         OperationStatus status = URLIdURL.put(null, keyEntry, dataEntry);
	         if (status != OperationStatus.SUCCESS) 
	         {
	                 throw new RuntimeException("Data insertion got status " +
	                                            status);
	         }
    	}
    	catch(Exception e)
    	{
    		 logger.error("Could not enter data in URLIdURL");
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
    public String retrieveFromURLIdURL(String URLId)
    {
    	boolean enteredCatch = false;
    	try
    	{
	        /* retrieve the data */
	        DatabaseEntry keyEntry = new DatabaseEntry();
	        DatabaseEntry dataEntry = new DatabaseEntry();
	        StringBinding.stringToEntry(URLId, keyEntry);
	        URLIdURL.get(null, keyEntry, dataEntry, null);
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
    		
             logger.error("Could not retreive data from URLIdURL");
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
    
    //End of insert and retrieve from URLID and URL table
    

    //Start of insert and retrieve from URLID and DocId
    
    public void insertIntoURLIdDocIdDB(String URLId,String docId)
    {
    	boolean enteredCatch = false;
    	try
    	{
	         DatabaseEntry keyEntry = new DatabaseEntry();
	         DatabaseEntry dataEntry = new DatabaseEntry();         
	         StringBinding.stringToEntry(URLId, keyEntry); 
	         StringBinding.stringToEntry(docId, dataEntry);
	         OperationStatus status = URLIdDocId.put(null, keyEntry, dataEntry);
	         if (status != OperationStatus.SUCCESS) 
	         {
	                 throw new RuntimeException("Data insertion got status " +
	                                            status);
	         }
    	}
    	catch(Exception e)
    	{
    		
            logger.error("Could not enter data in URLIdDocId");
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
    public String retrieveFromURLIdDocIdDB(String URLId)
    {
    	boolean enteredCatch = false;
    	try
    	{
	        DatabaseEntry keyEntry = new DatabaseEntry();
	        DatabaseEntry dataEntry = new DatabaseEntry();
	        StringBinding.stringToEntry(URLId, keyEntry);
	        URLIdDocId.get(null, keyEntry, dataEntry, null);
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
    		logger.error("Could not retrieve data from URLIdDocId");
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

    //End of insert and retrieve from URLID and DOCId table
    
    
    //Start of insert and retrieve from URL ID and document content table
     
    public void insertIntoDocIdDocContentDB(String DocId,String docContent)
    {
    	boolean enteredCatch = false;
    	try
    	{
	         DatabaseEntry keyEntry = new DatabaseEntry();
	         DatabaseEntry dataEntry = new DatabaseEntry();         
	         StringBinding.stringToEntry(DocId, keyEntry); 
	         StringBinding.stringToEntry(docContent, dataEntry);
	         OperationStatus status = DocIdDocContent.put(null, keyEntry, dataEntry);
	         if (status != OperationStatus.SUCCESS) 
	         {
	                 throw new RuntimeException("Data insertion got status " +
	                                            status);
	         }
    	}
    	catch(Exception e)
    	{
    		logger.error("Could not enter data in DocIdDocContent");
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
    
    public String retrieveFromDocIdDocContentDB(String URLId)
    {
    	boolean enteredCatch = false;
        try
        {
	        DatabaseEntry keyEntry = new DatabaseEntry();
	        DatabaseEntry dataEntry = new DatabaseEntry();
	        StringBinding.stringToEntry(URLId, keyEntry);
	        DocIdDocContent.get(null, keyEntry, dataEntry, null);
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
    		logger.error("Could not retrieve data from DocIdDocContent");
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
    
    //End of insert and retrieve from URLID and DOCID table

    
    //Start of insert and retrieve from URLId and when last crawled in the table
    
    public void insertIntoHostIdWhenLastCrawledDB(String URLId,Long whenLastCrawled)
    {
    	boolean enteredCatch = false;
    	try
    	{
	         DatabaseEntry keyEntry = new DatabaseEntry();
	         DatabaseEntry dataEntry = new DatabaseEntry();         
	         StringBinding.stringToEntry(URLId, keyEntry); 
	         LongBinding.longToEntry(whenLastCrawled, dataEntry);
	         OperationStatus status = HostIdWhenLastCrawled.put(null, keyEntry, dataEntry);
	         if (status != OperationStatus.SUCCESS) 
	         {
	                 throw new RuntimeException("Data insertion got status " +
	                                            status);
	         }
    	}
    	catch(Exception e)
    	{
    		logger.error("Could not enter data in HostIdWhenLastCrawled");
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
    
    public Long retrieveFromHostIdWhenLastCrawledDB(String URLId)
    {
    	boolean enteredCatch = false;
        try
        {
	        DatabaseEntry keyEntry = new DatabaseEntry();
	        DatabaseEntry dataEntry = new DatabaseEntry();
	        StringBinding.stringToEntry(URLId, keyEntry);
	        HostIdWhenLastCrawled.get(null, keyEntry, dataEntry, null);
	    	if(dataEntry.getData() != null)
	    	{
	    		Long data =LongBinding.entryToLong(dataEntry);
	    		return data;
	    	}
	    	else 
	    		return null;
        }
    	catch(Exception e)
    	{
    		logger.error("Could not retreive data from HostIdWhenLastCrawled");
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
    
    
    //End of insert and retrieve from URLID and when last crawled in the table
    

    
  //Start of insert and retrieve from URID and URLsTO  in the table
    public void insertIntoURLIDURLToDB(String URLId,String URLsTo)
    {
    	boolean enteredCatch = false;
    	try
    	{
	         DatabaseEntry keyEntry = new DatabaseEntry();
	         DatabaseEntry dataEntry = new DatabaseEntry();         
	         StringBinding.stringToEntry(URLId, keyEntry); 
	         StringBinding.stringToEntry(URLsTo, dataEntry);
	         OperationStatus status = URLIdURLto.put(null, keyEntry, dataEntry);
	         if (status != OperationStatus.SUCCESS) 
	         {
	                 throw new RuntimeException("Data insertion got status " +
	                                            status);
	         }
    	}
    	catch(Exception e)
    	{
    		logger.error("Could not enter data in URLIdURLto");
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
    public String retrieveFromURLFromURLToDB(String URLId)
    {
    	boolean enteredCatch = false;
        try
        {
	    	DatabaseEntry keyEntry = new DatabaseEntry();
	        DatabaseEntry dataEntry = new DatabaseEntry();
	        StringBinding.stringToEntry(URLId, keyEntry);
	        URLIdURLto.get(null, keyEntry, dataEntry, null);
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
    		logger.error("Could not retrieve data from URLIdURLto");
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
    public void cleanUP()
    {
    	
    }	
    //End of insert and retrieve from URID and URLsTO  in the table

    
    
    //Start of insert and retrieve from DocId to URLId  in the table
    public void insertIntoDocIdURLIdToDB(String DocId,String URLId)
    {
    	boolean enteredCatch = false;
    	try
    	{
	         DatabaseEntry keyEntry = new DatabaseEntry();
	         DatabaseEntry dataEntry = new DatabaseEntry();         
	         StringBinding.stringToEntry(DocId, keyEntry); 
	         
	         String data  = retrieveFromDocIdURLIdDB(DocId);
	         if(data != null)
	         {
	        	 URLId = URLId+";"+data;
	         }
	         StringBinding.stringToEntry(URLId, dataEntry);
	         OperationStatus status = DocIdURLId.put(null, keyEntry, dataEntry);
	         if (status != OperationStatus.SUCCESS) 
	         {
	                 throw new RuntimeException("Data insertion got status " +
	                                            status);
	         }
    	}
    	catch(Exception e)
    	{
    		
            logger.error("Could not enter data in DocIdURLIdTo");
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
    public String retrieveFromDocIdURLIdDB(String DocId)
    {
    	boolean enteredCatch = false;
        try
        {
	        DatabaseEntry keyEntry = new DatabaseEntry();
	        DatabaseEntry dataEntry = new DatabaseEntry();
	        StringBinding.stringToEntry(DocId, keyEntry);
	        DocIdURLId.get(null, keyEntry, dataEntry, null);
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
    		
            logger.error("Could not retrieve data from DocIdURLIdTo");
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
    
    //End of insert and retrieve from DocId to URLId in the table
    
    
    //Start of insert and retrieve from hostID and delay the table
    public void insertIntoHostIdCrawlDelayDB(String hostId,Long delay)
    {
    	boolean enteredCatch = false;
    	try
    	{
	         DatabaseEntry keyEntry = new DatabaseEntry();
	         DatabaseEntry dataEntry = new DatabaseEntry();         
	         StringBinding.stringToEntry(hostId, keyEntry); 
	         LongBinding.longToEntry(delay, dataEntry);
	         OperationStatus status = HostIdCrawlDelay.put(null, keyEntry, dataEntry);
	         if (status != OperationStatus.SUCCESS) 
	         {
	                 throw new RuntimeException("Data insertion got status " +
	                                            status);
	         }
    	}
    	catch(Exception e)
    	{
    		
            logger.error("Could not enter data in HostIdCrawlDelay");
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
    public Long retrieveFromHostIdCrawlDelayDB(String hostId)
    {
    	boolean enteredCatch = false;
    	try
    	{
	        DatabaseEntry keyEntry = new DatabaseEntry();
	        DatabaseEntry dataEntry = new DatabaseEntry();
	        StringBinding.stringToEntry(hostId, keyEntry);
	        HostIdCrawlDelay.get(null, keyEntry, dataEntry, null);
	    	if(dataEntry.getData() != null)
	    	{
	    		Long data = LongBinding.entryToLong(dataEntry);
	    		return data;
	    	}
	    	else 
	    		return null;
    	}
    	catch(Exception e)
    	{
    		
            logger.error("Could not retrieve data from HostIdCrawlDelay");
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
    
    //End of insert and retrieve from hostID and delay the table
    
    
  //Start of insert and retrieve from URLId and Disallow table
    
    public void insertIntoHostIdDisallow(String hostHash,String URLS)
    {
    	boolean enteredCatch = false;
    	try
    	{
         DatabaseEntry keyEntry = new DatabaseEntry();
         DatabaseEntry dataEntry = new DatabaseEntry();         
         StringBinding.stringToEntry(hostHash, keyEntry); 
         StringBinding.stringToEntry(URLS, dataEntry);
         OperationStatus status = HostIdDisallow.put(null, keyEntry, dataEntry);
         if (status != OperationStatus.SUCCESS) 
         {
                 throw new RuntimeException("Data insertion got status " +
                                            status);
         }
    	}
    	catch(Exception e)
    	{
    		
            logger.error("Could not enter data in HostIdDisallow");
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
    public boolean shouldDisallow(String hostHash,String absurl)
    {
    	boolean enteredCatch = false;
    	try
    	{
	    	String sep = "!!!!!@@@@@@@@!!!!!!@@@@@@!!!!!@@@!!!";
	        /* retrieve the data */
	        DatabaseEntry keyEntry = new DatabaseEntry();
	        DatabaseEntry dataEntry = new DatabaseEntry();
	        StringBinding.stringToEntry(hostHash, keyEntry);
	        HostIdDisallow.get(null, keyEntry, dataEntry, null);
	       
	    	if(dataEntry.getData() != null)
	    	{
	    		String data =StringBinding.entryToString(dataEntry);
	    		String [] urls = data.split(sep);
	    		for(int i=0;i<urls.length;i++)
	    		{
	    			 
	    			if(absurl.toLowerCase().startsWith(urls[i].toLowerCase()))
	    			{
	    					return true;
	    			}
	    			else 
	    			{
	    				String a = absurl.toLowerCase()+"/";
	    				if(a.startsWith(urls[i].toLowerCase()))
	    				return true; 
	    			}
	    		}
	    		return false;
	    	}  	
	    	else 
	    		return false;
    	}
    	catch(Exception e)
    	{
    		
            logger.error("Could not retrieve data in HostIdDisallow");
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
    	return false;
    }
    
    //End of insert and retrieve from URLID and Disallow table
    
    
  //Start of insert and retrieve from URLId and allow table
    
    public void insertIntoHostIdAllow(String hostHash,String URLS)
    {
    	boolean enteredCatch = false;
    	try
    	{
	         DatabaseEntry keyEntry = new DatabaseEntry();
	         DatabaseEntry dataEntry = new DatabaseEntry();         
	         StringBinding.stringToEntry(hostHash, keyEntry); 
	         StringBinding.stringToEntry(URLS, dataEntry);
	         OperationStatus status = HostIdAllow.put(null, keyEntry, dataEntry);
	         if (status != OperationStatus.SUCCESS) 
	         {
	                 throw new RuntimeException("Data insertion got status " +
	                                            status);
	         }
    	}
    	catch(Exception e)
    	{
    		logger.error("Could not enter data in HostIdAllow");
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
    public boolean shouldAallow(String hostHash,String absurl)
    {
    	boolean enteredCatch = false;
    	try
    	{
	    	String sep = "!!!!!@@@@@@@@!!!!!!@@@@@@!!!!!@@@!!!";
	        /* retrieve the data */
	        DatabaseEntry keyEntry = new DatabaseEntry();
	        DatabaseEntry dataEntry = new DatabaseEntry();
	        StringBinding.stringToEntry(hostHash, keyEntry);
	        HostIdAllow.get(null, keyEntry, dataEntry, null);
	        
	    	if(dataEntry.getData() != null)
	    	{
	    		String data =StringBinding.entryToString(dataEntry);
	    		String [] urls = data.split(sep);
	    		for(int i=0;i<urls.length;i++)
	    		{
	    			
	    			if(absurl.toLowerCase().startsWith(urls[i].toLowerCase()))
	    					return true;
	    			else if((absurl.toLowerCase()+"/").startsWith(urls[i].toLowerCase()))
	    			{
	    				return true; 
	    			}
	    		}
	    		return false;
	    	}  	
	    	else 
	    		return false;
    	}
    	catch(Exception e)
    	{
    		logger.error("Entered catch: closing all tables.");
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
    	return false;
    }
    
    //End of insert and retrieve from URLID and allow table
    
    
  /*****************************************************************************************/
    //For images
    
    
    public void insertIntoURLIdURLImg(String URLIdImg,String URLImg)
    {
    	boolean enteredCatch = false;
    	try
    	{
	         DatabaseEntry keyEntry = new DatabaseEntry();
	         DatabaseEntry dataEntry = new DatabaseEntry();         
	         StringBinding.stringToEntry(URLIdImg, keyEntry); 
	         StringBinding.stringToEntry(URLImg, dataEntry);
	         OperationStatus status = URLIdURLImg.put(null, keyEntry, dataEntry);
	         if (status != OperationStatus.SUCCESS) 
	         {
	                 throw new RuntimeException("Data insertion got status " +
	                                            status);
	         }
    	}
    	catch(Exception e)
    	{
    		logger.error("Could not enter data in URLIdURLImg");
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
    public String retrieveFromURLIdURLImg(String URLIdImg)
    {
    	boolean enteredCatch = false;
    	try
    	{
	        /* retrieve the data */
	        DatabaseEntry keyEntry = new DatabaseEntry();
	        DatabaseEntry dataEntry = new DatabaseEntry();
	        StringBinding.stringToEntry(URLIdImg, keyEntry);
	        URLIdURLImg.get(null, keyEntry, dataEntry, null);
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
    		logger.error("Could not retrieve data from URLIdURLImg");
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
    
    
    
    
    public void insertIntoURLIdContentImgDB(String URLIdImg,String contentImg)
    {
    	boolean enteredCatch = false;
    	try
    	{
	         DatabaseEntry keyEntry = new DatabaseEntry();
	         DatabaseEntry dataEntry = new DatabaseEntry();         
	         StringBinding.stringToEntry(URLIdImg, keyEntry); 
	         StringBinding.stringToEntry(contentImg, dataEntry);
	         OperationStatus status = URLIdContentImg.put(null, keyEntry, dataEntry);
	         if (status != OperationStatus.SUCCESS) 
	         {
	                 throw new RuntimeException("Data insertion got status " +
	                                            status);
	         }
    	}
    	catch(Exception e)
    	{
    		logger.error("Could not enter data in URLIdContentImg");
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
    
    public String retrieveFromURLIdContentImgDB(String URLIdImg)
    {
    	boolean enteredCatch = false;
        try
        {
	        DatabaseEntry keyEntry = new DatabaseEntry();
	        DatabaseEntry dataEntry = new DatabaseEntry();
	        StringBinding.stringToEntry(URLIdImg, keyEntry);
	        URLIdContentImg.get(null, keyEntry, dataEntry, null);
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
    		logger.error("Could not retrieve data from URLIdContentImg");
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
    
    
  /****************************************************************************************/  
    
    
    public void closeDBTables()
    {
    	
        
        //Close all the five tables that were created
    	URLIdURL.close();
        URLIdDocId.close();
        DocIdDocContent.close();
        HostIdWhenLastCrawled.close();
        URLIdURLto.close();
        DocIdURLId.close();
        HostIdCrawlDelay.close();
        HostIdDisallow.close();
        HostIdAllow.close();
        URLIdURLImg.close();
        URLIdContentImg.close();
        logger.info("All tables closed.");


    }
    
    public void closeEnvironment()
    {
    	crawlerStore.close();
    }   

    public static void main(String argv[]) throws IOException
    {
        DatabaseEntry keyEntry = new DatabaseEntry();
        DatabaseEntry dataEntry = new DatabaseEntry();
        CrawlerStore cs = new CrawlerStore("/home/cis455/worker/Rama/store");
        cs.openEnv();
        cs.openDBTables();
        Cursor cursor = cs.URLIdURL.openCursor(null, null);
        int i=0;
        while (cursor.getNext(keyEntry, dataEntry,null) == OperationStatus.SUCCESS) 
        {    	
        	String key = StringBinding.entryToString(keyEntry);
        	String data = StringBinding.entryToString(dataEntry);
        	//if(data.contains(".pdf"))
        	{
	            System.out.println("key=" + key + " data=" + data);
	            i++;
        	}
        }
        
       // System.out.println(cs.retrieveFromDocIdDocContentDB(cs.retrieveFromURLIdDocIdDB("fceba3b3adebbc3b7a484d1e6e734406ffe96574")));
        
        System.out.println("Count "+i);


        cursor.close();
        //System.out.println(cs.retrieveFromURLIdURL(AllStringsCrawler.convertToSHAHash("http://crawltest.cis.upenn.edu/")));
        cs.closeDBTables();
        cs.closeEnvironment();
        
    }
    
        
}
