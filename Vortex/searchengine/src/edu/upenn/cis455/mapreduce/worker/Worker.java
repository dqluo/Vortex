package edu.upenn.cis455.mapreduce.worker;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import edu.upenn.cis455.mapreduce.AllStrings;
import edu.upenn.cis455.mapreduce.storage.RankerStore;

public class Worker 
{
	public static boolean isMapFirstTime = true;
	public static String workerIPport;
	public static int workerport;
	public static String status = "none";
	public static int masterport;
	public static String masterhost;
	public static int noOfMapThreads;
	public static String ipdirectory;
	public static String storagedir;
	public static int noOfWorkers;
	public static String[] IPPort ;
	public static int me;
    public static PrintWriter writer;
	public static String databasestore;
	public static String indexstore;
	public static String rankstore;
	public static Double noOfDocuments;
	public static Map<String, Double> valuesAfterRank = new HashMap<String, Double>();
	public static void initializeFilePaths()
	{
		  if(Worker.storagedir.endsWith("/"))
		  {
			  Worker.storagedir= Worker.storagedir.substring(0, Worker.storagedir.length()-1);
		  }
		  if(Worker.ipdirectory.startsWith("/"))
		  {
			  Worker.ipdirectory = Worker.ipdirectory.substring(0, Worker.ipdirectory.length()-1);
		  }
		  Worker.ipdirectory = Worker.storagedir +"/"+Worker.ipdirectory;
		  Worker.databasestore = Worker.ipdirectory+"/store";

	}

	public static void getCumRank(String data,RankerStore rankerstore)
	{
		GetDocs.invokeDocs(data); 
		ArrayList<Thread> alOfThreads = new ArrayList<Thread>();
		  for(int i=0;i<10;i++)
		  {
			  Thread t = new Thread(new CalculationHandler(rankerstore));
			  alOfThreads.add(t);
			  t.start();
		  }
		  boolean allAreDead= false;

		  while(!allAreDead)
		  {
			  allAreDead = checkIfAllThreadsAreDead(alOfThreads);
		  }
		
		  //clearing elements:
		GetDocs.docs.clear();
		GetDocs.idf=0.0;
		GetDocs.globalCounterForDocs=1;

		
	}
	
	
	public static String sortDataForOneQueryTerm(Map<String, Double> h, String word)
	{
		Set<Entry<String, Double>> set = h.entrySet();
        List<Entry<String, Double>> list = new ArrayList<Entry<String, Double>>(set);
        Collections.sort( list, new Comparator<Map.Entry<String, Double>>()
        {
            public int compare( Map.Entry<String, Double> o1, Map.Entry<String, Double> o2 )
            {
                return (o2.getValue()).compareTo( o1.getValue() );
            }
        } );
	
        int i=1;
        StringBuilder sb = new StringBuilder();
        sb.append(word);
        sb.append("\t");
    	for(Entry<String, Double> entry:list)
    	{
        	//TODO ten is the number of pages we are returning per word
        	if(i>100)
        		break;
        	String append = entry.getKey();
        	String totalToAppend = String.valueOf(entry.getValue());
        	sb.append(append);
        	sb.append(AllStrings.urlSeperator);
        	sb.append(totalToAppend);
        	i++;
        }
        
        return sb.toString();
	}
	
	
	synchronized public static double getPageRank(String docID, double total,RankerStore rstore)
	{
		//TODO: add code for retrieving page rank: right now added 10
		
		//if page rank is 0 then add 0.10 to the result. 
		/*RankerStore rstore = new RankerStore(Worker.rankstore);
		rstore.openEnv();
		rstore.openDBTables();
		*/
		String pagerankString = rstore.retrieveFromRanker(docID);
		double pagerank = 0.0;
		
		if(pagerankString == null)
			pagerank = 0.10;
		else
			pagerank = Double.parseDouble(pagerankString);
		
		return total + pagerank;
	}

	  
	  public static boolean checkIfAllThreadsAreDead(ArrayList<Thread> alOfThreads )
	  {
			for(int i = 0; i<alOfThreads.size();i++)
			{
				  if(alOfThreads .get(i).isAlive())
				  {
			//		  System.out.println("Wait and push"+Worker.waitAndPushThreadName "Alive  "+alOfThreads.get(i).getName());
					  return false;
				  }
			}
			return true;
	  }
	
}
