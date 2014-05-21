package edu.upenn.cis455.mapreduce;


import java.io.IOException;

import edu.upenn.cis455.mapreduce.worker.Worker;

public class BasicTesting 
{
	public static void main(String args[]) throws IOException
	{
		//TO test copying of files
		//Worker.initializeFilePaths();
	/*	Worker.ipdirectory = "";
		  Worker.spoolinPath = "/home/cis455/worker/Rama"+"/spool-in";
		  Worker.seedPath = "/home/cis455/worker/Rama"+"/seed";
		  File f = new File(Worker.seedPath+"/seed.txt");
		  f.delete();
		  File f1 = new File(Worker.seedPath+"/seed.txt");
		Worker.copyFile(new File(Worker.spoolinPath+"/seed.txt"),f1);*/
	/*	URL u = new URL("http://en.wikipedia.org/wiki/Main_Page#p-search".trim());
		//String path = getPath(u.getHost());
		WorkerContext wc = new WorkerContext();
		Worker.noOfWorkers =2;
		Worker.ipdirectory = "/home/cis455/worker/Rama";
		System.out.println(wc.getPath(u.getHost()));
		String content = Worker.getPushContent(1);
		Worker.pushPost("127.0.0.1",8011,content);
		*/
		
		System.out.println("http://en.wikipedia.org/wiki/Main_Page :"+WorkerContext.checkAndRedirectRange("en.wikipedia.org"));
		System.out.println("http://www.dmoz.org/ :"+WorkerContext.checkAndRedirectRange("dmoz.org"));
		System.out.println("http://stackexchange.com/ :"+WorkerContext.checkAndRedirectRange("stackexchange.com"));
		System.out.println("http://www.howstuffworks.com/ :"+WorkerContext.checkAndRedirectRange("howstuffworks.com"));
		System.out.println("http://www.amazon.com/ :"+WorkerContext.checkAndRedirectRange("amazon.com"));
		System.out.println("https://www.bing.com/ :"+WorkerContext.checkAndRedirectRange("bing.com"));
		System.out.println("http://www.merriam-webster.com/ :"+WorkerContext.checkAndRedirectRange("merriam-webster.com"));
		System.out.println("http://dictlionary.reference.com/ :"+WorkerContext.checkAndRedirectRange("dictionary.reference.com"));	
;	    System.out.println("http://www.ebay.com/ :"+WorkerContext.checkAndRedirectRange("ebay.com"));	
		System.out.println("http://www.infoplease.com/ :"+WorkerContext.checkAndRedirectRange("infoplease.com"));	
		System.out.println("https://www.yahoo.com/ :"+WorkerContext.checkAndRedirectRange("yahoo.com"));
		System.out.println("http://www.mahalo.com/ :"+WorkerContext.checkAndRedirectRange("mahalo.com"));		
		System.out.println("https://www.google.com/ :"+WorkerContext.checkAndRedirectRange("google.com"));
		System.out.println("http://stackoverflow.com/ :"+WorkerContext.checkAndRedirectRange("stackoverflow.com"));		
		System.out.println("http://www.whitepages.com/ :"+WorkerContext.checkAndRedirectRange("whitepages.com"));
		System.out.println("http://www.anywho.com/whitepages :"+WorkerContext.checkAndRedirectRange("anywho.com"));
		System.out.println("http://www.encyclopedia.com/ :"+WorkerContext.checkAndRedirectRange("encyclopedia.com"));		
		System.out.println("http://www.thefreedictionary.com/ :"+WorkerContext.checkAndRedirectRange("thefreedictionary.com"));
		System.out.println("http://www.nytimes.com/ :"+WorkerContext.checkAndRedirectRange("nytimes.com"));			
				
				
	}
		

}