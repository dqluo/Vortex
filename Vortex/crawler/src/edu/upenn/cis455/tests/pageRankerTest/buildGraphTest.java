package edu.upenn.cis455.tests.pageRankerTest;

import org.junit.Test;

import edu.upenn.cis455.mapreduce.storage.CrawlerStore;
import edu.upenn.cis455.pageRanker.Ranker;

public class buildGraphTest {

	@Test
	public void printgraph() {
		Ranker r = new Ranker();
		r.buildGraph();
		r.printGraph();
	}
	
	@Test
	public void printRanks() {
		Ranker r = new Ranker();
		r.buildGraph();
		r.accumulateRanks();
		r.printRanks();
	}

	@Test
	public void testUrlFromToIterator() {
		CrawlerStore crawlStore = new CrawlerStore("/Users/karthikalle/Desktop/CIS 555/EMR/Project/BDBStore");
		crawlStore.openEnv();
		crawlStore.openDBTables();
		//crawlStore.iterateURLFromURLToDB();	
		
/*		try {
			System.out.println(URLDecoder.decode(crawlStore.retrieveFromURLIdURL("a328ddc0e26fba14d801e32aab1cb484deb56193"),"UTF-8"));
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		
*/		crawlStore.closeDBTables();
		crawlStore.closeEnvironment();
	}
}
