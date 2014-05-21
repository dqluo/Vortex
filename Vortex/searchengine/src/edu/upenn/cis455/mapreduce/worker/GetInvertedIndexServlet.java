package edu.upenn.cis455.mapreduce.worker;


import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import edu.upenn.cis455.mapreduce.AllStrings;
import edu.upenn.cis455.mapreduce.storage.IndexerImageStore;
import edu.upenn.cis455.mapreduce.storage.IndexerStore;
import edu.upenn.cis455.mapreduce.storage.RankerStore;


public class GetInvertedIndexServlet extends HttpServlet 
{
	static Logger loggerRunmap= Logger.getLogger(GetInvertedIndexServlet.class);
	
	
	public void doPost(HttpServletRequest request, HttpServletResponse response) throws java.io.IOException
    {
		IndexerStore istore = null;
		RankerStore rstore = null;
		
		
		try
		{
		String word = request.getParameter(AllStrings.wordForInvertedIndexString);

		istore = new IndexerStore(Worker.indexstore);
		 rstore = new RankerStore(Worker.rankstore);
		 
		
		//System.out.println(Worker.indexstore);
		
		
		rstore.openEnv();
		rstore.openDBTables();
		istore.openEnv();
		istore.openDBTables();
		
		if(word != null)
		{
			String invertedIndex= istore.retrieveFromIndexer(word);
			//System.out.println("I am inverted index"+invertedIndex);
			if(invertedIndex != null && !invertedIndex.equals("null"))
			{
				
				Worker.getCumRank(invertedIndex,rstore);
				
				String sendToMaster = Worker.sortDataForOneQueryTerm(Worker.valuesAfterRank, word);
				//System.out.println("send data to master: "+sendToMaster);
				//Worker.writer.write(sendToMaster);
				//Worker.writer.close();
				WorkerStatus.reportResult(sendToMaster);
				Worker.valuesAfterRank.clear();
				
			}
			else
			{
				WorkerStatus.reportResult(word+"\tnull");
				Worker.valuesAfterRank.clear();
			}
		}
		else
		{
			WorkerStatus.reportResult(word+"\tnull");
			Worker.valuesAfterRank.clear();
		}
		istore.closeDBTables();
		istore.closeEnvironment();
		//istore = null;

			rstore.closeDBTables();
			rstore.closeEnvironment();
			//rstore = null;

		Worker.valuesAfterRank.clear();
    }
	finally
	{
		/*if(istore != null)
		{
			istore.closeDBTables();
			istore.closeEnvironment();
		}
		
		if(rstore != null)
		{
			rstore.closeDBTables();
			rstore.closeEnvironment();
		}*/
		Worker.valuesAfterRank.clear();
	}
}

}
