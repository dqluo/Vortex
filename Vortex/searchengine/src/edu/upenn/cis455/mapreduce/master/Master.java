package edu.upenn.cis455.mapreduce.master;

import java.io.DataOutputStream;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;

import org.apache.log4j.Logger;
import edu.washington.cs.knowitall.morpha.MorphaStemmer;
import edu.upenn.cis455.mapreduce.AllStrings;
import edu.upenn.cis455.mapreduce.WorkerContext;
import edu.upenn.cis455.mapreduce.worker.Worker;

public class Master 
{
	public static HashMap<Integer,WorkerInMaster> workerStatus = new HashMap<Integer,WorkerInMaster>();
	public static int noOfActiveWorkers;
	public static String ipdirectory;
	public static int noOfMapThreads;
	public static boolean didMasterIssue= false;
	public static boolean isMasterWaiting = false;
	public static boolean allInvertedIndicesReceived = false;
	public static long timeOfRequestIssued;
	public static long timeOfAllAcksReceived;
    public static PrintWriter writer;
	
    
	static Logger logger = Logger.getLogger(Master.class);
	public static HashMap<String,Integer> wordWorkerNumber = new HashMap<String,Integer>();
	public static HashMap<Integer,String> wordNumberWord = new HashMap<Integer,String>();
	public static HashMap<String,Boolean> wordInvIndexReceived = new HashMap<String,Boolean>();
	public static HashMap<String,Boolean> wordRequestSent = new HashMap<String,Boolean>();
	
	public static HashMap<Integer,String> query = new HashMap<Integer,String>();
	public static ArrayList<String> urlsToDisplay = new ArrayList<String>();
	
	//Should I create a database for this??
	public static HashMap<String,String> wordInvIndex = new HashMap<String,String>();
	
	
	public static HashMap<Integer,Boolean> workerNumberRequestSent = new HashMap<Integer,Boolean>();
	
	
	public static int noOfRequestsForInvertedIndexSent_loop =0;
	public static int noOfRequestsForInvertedIndexSent;

	public static ArrayList<String> toBeSent = new ArrayList<String>();
	
	public void setFormParameters(String ipdirectory,int noOfMapThreads)
	{
		Master.ipdirectory = ipdirectory;
		Master.noOfMapThreads = noOfMapThreads;
	}
	
	public static void splitAndHash(String queryString)
	{
		setActiveWorkers();
		Worker.noOfWorkers = Master.noOfActiveWorkers;
//		Worker.noOfWorkers = 2;		
		String [] queryStringWords = queryString.split(" ");
		if(queryStringWords == null || queryStringWords.length == 0)
			return;
		for(int i = 0;i<queryStringWords.length;i++)
		{
			//System.out.println("no of times looped" + i);
			if(queryStringWords[i].isEmpty())
				continue;
			String currentWord = queryStringWords[i].toLowerCase();
			if(AllStrings.stopWords.get(currentWord) != null)
			{   
				//Mark position for stop words
				query.put(i, null);
				continue;
			}
				
			
			Matcher matcher = AllStrings.acceptedPatternOfKeyWords.matcher(currentWord);
			
			if(!matcher.find() && matcher.group().length()!=currentWord.length())
			{				
				//Mark position for alphanumeric
				query.put(i, null);
				continue;
			}
			currentWord=MorphaStemmer.morpha(currentWord,false);
			int workerNumber = WorkerContext.checkAndRedirectRange(currentWord);
			//Putting values in word Number and word
			wordNumberWord.put(i, currentWord);
			query.put(i, currentWord);
			//Putting values in worker number and word
			//If a request for a word has not been sent then sending a request for inverted index
			if(!wordRequestSent.containsKey(currentWord))
			{
				System.out.println("Worker :  "+workerNumber);
				System.out.println("Word :    "+currentWord);
				noOfRequestsForInvertedIndexSent++;
				noOfRequestsForInvertedIndexSent_loop++;
				wordWorkerNumber.put(currentWord,workerNumber);
				wordInvIndexReceived.put(currentWord,false);
				//System.out.println("Current word "+ currentWord);
				//System.out.println(workerNumberRequestSent);
				//System.out.println(wordWorkerNumber);
				if(workerNumberRequestSent.get(WorkerContext.checkAndRedirectRange(currentWord)) == null 
						|| workerNumberRequestSent.get(WorkerContext.checkAndRedirectRange(currentWord)) ==  false)
				{
					Master.callRequestForInvertedIndex(workerNumber,currentWord);
					workerNumberRequestSent.put(WorkerContext.checkAndRedirectRange(currentWord),true);
					wordRequestSent.put(currentWord, true);
				}
				else
				{
					toBeSent.add(currentWord);
				}
			}
		}
		//System.out.println("This should be four "+noOfRequestsForInvertedIndexSent_loop);
		//System.out.println("The map query terms contains:");
		/*for(int key : query.keySet())
			System.out.println(key+": "+query.get(key));
		*/
		for(int i=0;i<toBeSent.size();i++)
		{
			System.out.println("To be sent "+toBeSent.get(i));
		}
		while(noOfRequestsForInvertedIndexSent_loop  > 0)
		{
			for(int i=0;i<toBeSent.size();i++)
			{
				String currentword = toBeSent.get(i);
				int workernumber = Master.wordWorkerNumber.get(currentword);
				if(Master.workerNumberRequestSent.get(workernumber) ==  false)
				{
					toBeSent.remove(i);
					Master.callRequestForInvertedIndex(workernumber,currentword);
					Master.workerNumberRequestSent.put(Master.wordWorkerNumber.get(currentword),true);
					wordRequestSent.put(currentword, true);
				}
			}
		}
	}
	
	public static void clearAllHashMaps()
	{
		Master.wordInvIndex.clear();
		Master.wordInvIndexReceived.clear();
		Master.wordNumberWord.clear();
		Master.wordRequestSent.clear();
		Master.wordWorkerNumber.clear();
		Master.workerNumberRequestSent.clear();
		Master.query.clear();
		Master.urlsToDisplay.clear();
		Master.noOfRequestsForInvertedIndexSent =0;
		Master.noOfRequestsForInvertedIndexSent_loop=0;
		Master.toBeSent.clear();
	}
	
	public static int getWorkerNumber(int port)
	{
		switch(port)
		{
			case 8011: return 1;
			case 8012: return 2;
			case 8013: return 3;
			case 8014: return 4;
			case 8015: return 5;
			case 8016: return 6;
			case 8017: return 7;
			case 8018: return 8;
			case 8019: return 9;
			case 8020: return 10;
		}
		return -1;
	}
	
	public static int getPort(int workerNumber)
	{
		switch(workerNumber)
		{
			case 1:  return 8011;
			case 2:  return 8012;
			case 3:  return 8013;
			case 4:  return 8014;
			case 5:  return 8015;
			case 6:  return 8016;
			case 7:  return 8017;
			case 8:  return 8018;
			case 9:  return 8019;
			case 10: return 8020;
		}
		return -1;
	}
	
	
	//To update status of worker from workerstatus
	public static void updateWorkerStatus(Integer workerPort,String workerhost,String status)
	{
		if(workerStatus.containsKey(workerPort))
		{
			WorkerInMaster w = workerStatus.get(workerPort);
			w.setWorkerHost(workerhost);
			w.setStatus(status);
			w.setLastEdited();
			w.setIsActive(true);
			workerStatus.put(workerPort, w);
		}
		else
		{
			WorkerInMaster w = new WorkerInMaster();
			w.setWorkerHost(workerhost);
			w.setStatus(status);
			w.setLastEdited();
			w.setIsActive(true);
			workerStatus.put(workerPort, w);
		}
	}
	//Function to correct or set workers status
	public static void setActiveWorkers()
	{
		Iterator<Entry<Integer, WorkerInMaster>> it = Master.workerStatus.entrySet().iterator();
		 Master.noOfActiveWorkers =0;
		while (it.hasNext()) 
		{
			Map.Entry<Integer,WorkerInMaster> pairs = (Map.Entry<Integer,WorkerInMaster>)it.next();
		   
		    WorkerInMaster w = pairs.getValue();
		    	//w.setIsActive(false);	
		    if(w.getLastEdited() <= System.currentTimeMillis()-30000)
		    {
		    		w.setIsActive(false);
		    }
		    else
		    {
		    	
		    	Master.noOfActiveWorkers++;
		    	w.setIsActive(true);
		    }
		  }
		}
		//Construct worker IP:Port string to send with runmap
	public static String constructIPPortString()
	{
		String result = "";
		Iterator<Entry<Integer, WorkerInMaster>> it = Master.workerStatus.entrySet().iterator();
		while (it.hasNext()) 
	    {
	    	Map.Entry<Integer,WorkerInMaster> pairs = (Map.Entry<Integer,WorkerInMaster>)it.next();
	    	WorkerInMaster w = pairs.getValue();
	    	if(pairs.getValue().getIsActive() == true)
	    	{
	    		
	    		String worker = "worker"+getWorkerNumber(pairs.getKey())+"=";
	    		if(!result.isEmpty())
	    		{
	    			result = result+"&"+worker+w.getWorkerHost()+":"+pairs.getKey();
	    		}
	    		else
	    		{
	    			result = worker+w.getWorkerHost()+":"+pairs.getKey();
	    		}
	    	}
	    }
		return result;
	}


	public static void callRequestForInvertedIndex(int workerNumber,String requestForInvertedIndex) 
	{
	  Socket soc = null;
		try
		{
			System.out.println(workerNumber +"  "+ requestForInvertedIndex);
			int port = getPort(workerNumber);		
			String workerhost = Master.workerStatus.get(port).workerhost;
			soc = new Socket(workerhost,port);
			String postBody;
			System.out.println("Worker host and ip"+workerhost+" "+port);
			postBody = AllStrings.wordForInvertedIndexString + "=" +requestForInvertedIndex;
		    DataOutputStream bw = new DataOutputStream(soc.getOutputStream()); 
			bw.writeBytes(AllStrings.httpMethodPOST+"/getinvertedindex"+" HTTP/1.1\r\n");
			bw.writeBytes(AllStrings.host+workerhost+"\r\n");
			bw.writeBytes(AllStrings.contentLengthString+postBody.length()+"\r\n");
			bw.writeBytes(AllStrings.contentTypeString+AllStrings.contentTypeValue+"\r\n");
			bw.writeBytes(AllStrings.connectionClosed+"\r\n\r\n");
			bw.writeBytes(postBody+"\r\n\r\n");
			bw.flush();
		    soc.close();
		}						
		catch (IOException e) 
		{
			//e.printStackTrace();
	
		}
		finally
		{
			try 
			{
				soc.close();
			} catch (IOException e) 
			{
				e.printStackTrace();
			}
		}
	}		    
	
	
	
	public static void startFetchingResults()
	{
	
		if(noOfRequestsForInvertedIndexSent == 0)
		{
			urlsToDisplay.add("Sorry! There are no results for your query!");
			return;
		}
		urlsToDisplay = getFinalURL(wordInvIndex, noOfRequestsForInvertedIndexSent);
		
		if(urlsToDisplay == null || urlsToDisplay.size() == 0)
		{
			urlsToDisplay.add("Sorry! There are no results for your query!");
		}
			
	}
	public static ArrayList<String> getFinalURL(HashMap<String, String> a, int queryTerms)
	{
		Map<String, Double> urlAndFinalRank = new HashMap<String, Double>();
		if(queryTerms==1)
		{
			
			String content = null;
			for(String key : a.keySet())
			{
				content = a.get(key);
				break;
			}
			if(content != null)
			{
				String docs[] = content.split("<@@@!REDUCESEPARATOR!@@@>"); 
				String url="";
				for(int i=1; i<docs.length; i++)
				{
					double newrank =0.0;
					String urlData = docs[i].split("<@!NEWSEPARATOR!@>")[1].trim();
					String data[] = urlData.split("<@!Separatorurl!@>");
					String rank;
					url = data[0];
					if(data.length >= 4)
						rank = data[3];
					else
						rank = "10.0";
					double numberOfTimesWordOccured=0.0;
					
					if(data[1].contains(";"))
					{
						String position[] = data[1].split(";");
						numberOfTimesWordOccured = position.length + 1;
					}
					
					else
						numberOfTimesWordOccured = 1;
					
					newrank += Double.parseDouble(rank) + (numberOfTimesWordOccured * 20);
					urlAndFinalRank.put(url, newrank);
					
				}
				
				urlsToDisplay = sort(urlAndFinalRank);
				
			}
			return urlsToDisplay;
		}
			
		else if(queryTerms > 1)
		{
			HashMap<String, HashMap<String, String>> allData = new HashMap<String, HashMap<String, String>>();
			allData = createForwardIndex(a);
			ArrayList <String> results = new ArrayList<String>();
			results = calculateRanks(allData);
			return results;
		}
		else
		{
			return null;
		}
	}
	
	
	public static HashMap<String, HashMap<String, String>> createForwardIndex(HashMap<String, String> orgMap)
	{
		HashMap<String, HashMap<String, String>> allData = new HashMap<String, HashMap<String, String>>();
		
		for(String word : orgMap.keySet())
		{
			String dataForWord = orgMap.get(word);
			
			if(dataForWord == null)
				continue;
			
			String docs[] = dataForWord.split("<@@@!REDUCESEPARATOR!@@@>"); 
			for(int i=1; i<docs.length; i++)
			{
				HashMap<String, String> wordAndData = new HashMap<String, String>();
				
				String data[] = docs[i].split("<@!NEWSEPARATOR!@>");
				
				if(allData.get(data[0])!=null)
					wordAndData = allData.get(data[0]);
				else
					wordAndData.clear();
				
				wordAndData.put(word, data[1]);
				allData.put(data[0], wordAndData);
				
			}
			
		}
		
		
		for(String docID : allData.keySet())
		{
			HashMap<String, String> test = new HashMap<String, String>();
			test = allData.get(docID);
			
		}
		
		
		return allData;
	}
	
	
	public static ArrayList<String> calculateRanks(HashMap<String, HashMap<String, String>> newMap)
	{
		/*for(String word : newMap.keySet())
		{
			System.out.println(word);
			HashMap<String, String> h = newMap.get(word);
			for(String key : h.keySet())
			{
				System.out.println(key+": "+h.get(key));
			}
			
		}*/
		
		HashMap<String, Double> urlAndFinalRank = new HashMap<String, Double>();
		
		for(String urlID : newMap.keySet())
		{
			HashMap<String, String> wordAndData = new HashMap<String, String>();
			
			wordAndData = newMap.get(urlID);
			
			if(wordAndData ==null)
				continue;
			
			if(wordAndData.size()==1)
			{
				double newrank = 0.0;
				for (String word : wordAndData.keySet())
				{
					String data[] = wordAndData.get(word).split("<@!Separatorurl!@>");
					String url = data[0];
					String rank = data[3];
					
					double numberOfTimesWordOccured=0.0;
					
					if(data[1].contains(";"))
					{
						String position[] = data[1].split(";");
						numberOfTimesWordOccured = position.length + 1;
					}
					
					else
						numberOfTimesWordOccured = 1;
					
					newrank += Double.parseDouble(rank) + (numberOfTimesWordOccured * 20);
					
					urlAndFinalRank.put(url, newrank);
				}
			}
			
			else 
			{
				double numberofMatchings = 0.0; 
				double newrank=0.0;
				
				String word1 = "";
				int posn1=0;
				for(int x= 0; x<query.size(); x++)
				{
					if(query.get(x)!=null)
					{
						word1=query.get(x);
						posn1=x;
						break;
					}
				}
				
				if(wordAndData.get(word1)==null)
					continue;
				String data[] = wordAndData.get(word1).split("<@!Separatorurl!@>");
				String url = data[0];
				String rank = data[3];
				String positions1 = data[1].replaceAll("pos:<", " ");
				String positions2 = positions1.replaceAll(">", " "); 
				
				double numberOfTimesWordOccured=0.0;
				
				String position[] = positions2.trim().split(";");
				
				if(!positions2.contains(";"))
				{
					numberOfTimesWordOccured = 2;
				}
				else
				{
					numberOfTimesWordOccured = position.length + 1;
				}
					
				newrank = Double.parseDouble(rank) + ((numberOfTimesWordOccured-1) * 20);
				
				for(int i= 0; i<numberOfTimesWordOccured-1; i++)
				{
					String pos = position[i];
					int posNum = Integer.parseInt(pos);
					
					for(int x= posn1+1; x<query.size(); x++)
					{
						if(query.get(x)!=null)
						{
							String word2=query.get(x);
							int posn2=x;
							int difference = posn2-posn1;
							//System.out.println("Why Exception"+wordAndData.get(word2));
							if(wordAndData.get(word2)==null)
								continue;
							String datanew[] = wordAndData.get(word2).split("<@!Separatorurl!@>");
							String ranknew = datanew[3];
							String positionsnew = datanew[1].trim();
							
							double toAdd=0.0;
							
							if(positionsnew.contains(";"))
							{
								toAdd = positionsnew.length() + 1;
							}
							else
							{
								toAdd = 1;
							}
							
							int n = posNum + difference;
							String positionShouldBe = String.valueOf(n);
							String check1 = "<"+positionShouldBe+">";
							String check2 = "<"+positionShouldBe+";";
							String check3 = ";"+positionShouldBe+";";
							String check4 = ";"+positionShouldBe+">";
							
							
							if(positionsnew.contains(check1) || positionsnew.contains(check2)
									|| positionsnew.contains(check3) || positionsnew.contains(check4))
							{
								numberofMatchings++;
								if(i==0)
									newrank += Double.parseDouble(ranknew) + (toAdd * 20);
								
							}
							
						}
					}
							
				}
				
				newrank = newrank/(wordAndData.size()) + (numberofMatchings * 50);
				//System.out.println("Number of matchings: "+numberofMatchings+" for url : "+url);
				urlAndFinalRank.put(url, newrank);
				
			}
			
		}
		
	
		urlsToDisplay = sort(urlAndFinalRank);
		
		return urlsToDisplay;
	}
	
	public static ArrayList<String> sort(Map<String, Double> a)
	{
		Set<Entry<String, Double>> set = a.entrySet();
        List<Entry<String, Double>> list = new ArrayList<Entry<String, Double>>(set);
        Collections.sort( list, new Comparator<Map.Entry<String, Double>>()
        {
            public int compare( Map.Entry<String, Double> o1, Map.Entry<String, Double> o2 )
            {
                return (o2.getValue()).compareTo( o1.getValue() );
            }
        } );
	
        int i = 0;
        ArrayList<String> anew = new ArrayList<String>();
        for(Entry<String, Double> entry:list){
        	i++;
        	anew.add(entry.getKey());
        	if(i==50)
        		break;
        }
		
		return anew;
	}

	
}
		

