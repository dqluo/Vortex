package edu.upenn.cis455.mapreduce.worker;

import java.util.ArrayList;
import java.util.Collections;

import edu.upenn.cis455.mapreduce.AllStrings;

public class GetDocs 
{
	 public static double idf=0.0;
	 public static int globalCounterForDocs = 1;
	 public static ArrayList<String> docs = new ArrayList<String>();
	public static void invokeDocs(String data)
	{
		data = data.trim();
		String docsArr[] = data.split(AllStrings.reduceSeperator);
		
		for(int i=1; i<docsArr.length; i++)
		{
			docs.add(docsArr[i]);
		}		
	 
		 //TODO write the correct number
		idf=Math.log(500000/docs.size());
	}
	synchronized public static String getADocument()
	{
		if(docs.size() > globalCounterForDocs)
			return docs.get(globalCounterForDocs++);
		else
			return null;

	}	

}
