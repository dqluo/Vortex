package edu.upenn.cis455.pageRanker;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;

import edu.upenn.cis455.mapreduce.storage.CrawlerStore;

public class Ranker {

	public HashMap<String, ArrayList<String>> webgraph;
	public HashMap<String, Float> ranks;

	public void buildGraph() {

		//Representation of the web graph
		webgraph = new HashMap<>();

		//Has all the ranks at the end of the process, and also contains all the urls
		ranks = new HashMap<>();

		File f = new File("/Users/karthikalle/Desktop/CIS 555/Project/Project/InputToRanker/BDBStoreURLs.txt");
		try {
			BufferedReader br = new BufferedReader(new FileReader(f));
			String line;
			while((line = br.readLine())!=null) {
				//System.out.println(line);
				String ids[] = line.split("\t");
				if(ids.length>=2) {
					String urlFrom = ids[0];
					String urlTo = ids[1];
					if(webgraph.containsKey(urlFrom)) {
						ArrayList<String> already = webgraph.get(ids[0]);
						already.add(urlTo);
						webgraph.put(urlFrom, already);
					}
					else {
						ArrayList<String> already = new ArrayList<String>();
						already.add(urlTo);
						webgraph.put(urlFrom, already);
					}
					//Initializing Ranks and has all the URLs
					ranks.put(urlFrom,(float)1);
					ranks.put(urlTo,(float)1);

					//		System.out.println("Rank of "+urlFrom+" = "+ranks.get(urlFrom));
				}
			}
			br.close();
		}
		catch(IOException e){
			e.printStackTrace();
		}
	}

	public void printGraph() {
		for(String u: webgraph.keySet()) {			
			System.out.println("URL "+u+" points to "+webgraph.get(u));

		}
	}


	public void accumulateRanks() {

		for(String n: ranks.keySet()) {
			//ranks.put(n,1/(float)ranks.size());
			//Putting a rank of 1 for everyone
			ranks.put(n,(float)1);
			//System.out.println("rank of "+n+" "+ranks.get(n));
		}

		System.out.println("");


		boolean stopFlag = false;
		int iteration1=0, iteration2=0;
		for(int i = 0; i<10; i++) {
			//while(!stopFlag) {
			iteration1 ++;
			stopFlag = computeRanks(stopFlag, true);
			//printRanks();
		}
		//printRanks();

		/*System.out.println("End of phase 1");
		stopFlag = false;
		//for(int i = 0; i<10; i++) {
		while(!stopFlag) {
			iteration2 ++;
			stopFlag = computeRanks(stopFlag, true);
		}*/
		System.out.println("Convergence1: "+iteration1);
		System.out.println("Convergence2: "+iteration2);
	}

	public boolean computeRanks(boolean stopFlag, boolean includeLeaves) {
		CrawlerStore cs = initCrawlerStore();

		HashMap<String, Float> newRanks = new HashMap<String, Float>();
		int numofNodes = 0;

		for(String parent: webgraph.keySet()) {
			//	System.out.println(cs.retrieveFromURLIdURL(parent));
			ArrayList<String> children = webgraph.get(parent);
			int numOfChildren = children.size();
			for(String child: children) {
				if(webgraph.containsKey(child)||includeLeaves) {

					if(cs.retrieveFromURLIdURL(child)!=null&&cs.retrieveFromURLIdURL(parent)!=null) {
						Float rankOfParent = ranks.get(parent);

						System.out.println("Parent "+cs.retrieveFromURLIdURL(parent)+" gives to it's child: "+cs.retrieveFromURLIdURL(child)+" a weight of "+rankOfParent/(float)numOfChildren);

						Float postRankOfChild;
						if(!newRanks.containsKey(child))
							postRankOfChild = (rankOfParent/(float)numOfChildren);
						else 
							postRankOfChild = newRanks.get(child) + (rankOfParent/(float)numOfChildren);

						newRanks.put(child, postRankOfChild);
					}
					/*	else {
						System.out.println("parent "+cs.retrieveFromURLIdURL(parent)+"\t"+"child: "+cs.retrieveFromURLIdURL(child));
						System.out.println("Null values of child and parent");
					}*/
				}
			}
		}

		stopCrawlStore(cs);

		int flag = 0;

		for(String s: ranks.keySet()) {
			if(!newRanks.containsKey(s)) {
				//	ranks.put(s, (float)0.15);
				continue;
			}
			numofNodes++;
			float newRank = (float)0.15 + (float)0.85*(newRanks.get(s));
			float difference = Math.abs(newRank-ranks.get(s));

			//	System.out.println(difference);
			if(difference>0.001){
				System.out.println("new rank"+newRank);
				ranks.put(s, newRank);
			}
			else
				flag ++;

		}
		//	System.out.println(flag);
		//	System.out.println(numofNodes);
		if(flag == numofNodes)
			stopFlag = true;

		/*for(int n = 1; n<=ranks.size(); n++)
			System.out.println("rank of "+n+" "+ranks.get(Integer.toString(n)));
		System.out.println("");*/	
		return stopFlag;
	}

	public ArrayList<String> getLeaves() {

		ArrayList<String> leaves = new ArrayList<>();
		for(String node: ranks.keySet()) {
			if(!webgraph.containsValue(node)) {
				leaves.add(node);
			}
		}
		return leaves;
	}

	public void printRanks() {
		CrawlerStore crawlStore = initCrawlerStore();

		HashMap<Float, String> reverseRanks = new HashMap<>();

		for(String nodes: ranks.keySet()) {

			String URL = crawlStore.retrieveFromURLIdURL((String)nodes);
			if(URL!=null) {
				Float rank = ranks.get(nodes);
				if(reverseRanks.containsKey(rank)) 
					reverseRanks.put(rank, reverseRanks.get(rank)+"\n"+URL);
				else 
					reverseRanks.put(rank, URL);
			}
		}

		Object[] keys = reverseRanks.keySet().toArray();
		Arrays.sort(keys, Collections.reverseOrder());
		for(Object key : keys) {
			//	System.out.println("Rank: "+(key));
			//System.out.println("Rank of ");
			File f = new File("/Users/karthikalle/Desktop/CIS 555/Project/Project/RankingResults/Result"+System.currentTimeMillis()+".txt");
			PrintWriter pw = null;
			try {
				pw = new PrintWriter(f);
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
			for(String each: reverseRanks.get(key).split("\n")){
				pw.write(each+": "+key+"\n");
				//	System.out.println(each+": "+key);
			}
		}
		stopCrawlStore(crawlStore);
	}

	public CrawlerStore initCrawlerStore() {
		CrawlerStore crawlStore = new CrawlerStore("/Users/karthikalle/Desktop/CIS 555/Project/Project/BDBStore");
		crawlStore.openEnv();
		crawlStore.openDBTables();
		return crawlStore;
	}

	public void stopCrawlStore(CrawlerStore crawlStore) {
		crawlStore.closeDBTables();
		crawlStore.closeEnvironment();
	}
}

