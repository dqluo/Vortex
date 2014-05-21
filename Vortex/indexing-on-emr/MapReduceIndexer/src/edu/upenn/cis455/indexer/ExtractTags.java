package edu.upenn.cis455.indexer;


import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.safety.Whitelist;
//import edu.upenn.cis455.storage.CrawlerStore;
import edu.washington.cs.knowitall.morpha.MorphaStemmer;


public class ExtractTags {
	 HashMap<String, Integer> maxCountMap;
		ArrayList<String> ar;
		HashMap<String, HashMap<String, ArrayList<Integer>>> everything1 ;
		Pattern pattern ;

	ExtractTags(){
		maxCountMap= new HashMap<String, Integer>();
		everything1 = new HashMap<String, HashMap<String, ArrayList<Integer>>>();
		ar=new ArrayList<String>();
		pattern = Pattern.compile("[a-z]*");
	}

	
	public HashMap<String, HashMap<String, ArrayList<Integer>>> getTags(String docid,String html,String url)
	{
		//String html="<html><head><title>I am the title yay</title></head><body><p>An <a href='http://example.com/'><b>example </b><p>hiiiiiiii</p><b>hello</b></a> link </p><i><b>example to,test.Blah!</b></i><h1> hello</h1><h2>yo</h2><h3>kDHK <h3/></body></html>";
		////System.out.println(docid+"docid");
		String arr[]={"i","me","my","myself","we","our","ours","ourselves","you","your","yours","yourself","yourselves","he","him","his","himself","she","her","hers","herself","it","its","itself","they","them","their","theirs","themselves","what","which","who","whom","this","that","these","those","am","is","are","was","were","be","been","being","have","has","had","having","do","does","did","doing","a","an","the","and","but","if","or","because","as","until","while","of","at","by","for","with","about","against","between","into","through","during","before","after","above","below","to","from","up","down","in","out","on","off","over","under","again","further","then","once","here","there","when","where","why","how","all","any","both","each","few","more","most","other","some","such","no","nor","not","only","own","same","so","than","too","very","s","t","can","will","just","don","should","now"};

		
		try {
			/*FileInputStream ins1;  	
			ins1 = new FileInputStream("english");
			BufferedReader br1 = new BufferedReader(new InputStreamReader(ins1));
			String strL;*/
			for(String strL:arr){
				////System.out.println(strL);
				ar.add(strL);

			}
			//get url
			String urlDecoded=	URLDecoder.decode(url, "UTF-8");
			URL findhost= new URL(urlDecoded);
			String hostName = findhost.getHost();
			
			//check for url
			addURLHostPositions(urlDecoded.toLowerCase(),"url");

			//check for domain
			addURLHostPositions(hostName.toLowerCase(),"host");
			
			//check for title
			Document doc1 = Jsoup.parse(html);
			int titleSize = doc1.select("title").size();
			////System.out.println(titleSize+"tagSize");
			for(int i=0; i<titleSize; i++)
			{
				Element tags = doc1.select("title").get(i);
				String tagText = tags.text().toLowerCase();
				addURLHostPositions(tagText,"title");
			}
			//if((cs.retrieveWofromWord(x)!=null)){
			//cleaning the document
			String safe = Jsoup.clean(html, Whitelist.relaxed());
			////System.out.println("safe"+safe+"endsafe");
			Document doc = Jsoup.parse(safe.toLowerCase());
			//hashmap to store values for each document
			
			

			//loop for bold words
			addTag(doc,"b");
			//loop for italic words
			addTag(doc,"i");
			
			
			//check for href
			addTag(doc,"a");
			//check for h1
			addTag(doc,"h1");
			//check for h2
			addTag(doc,"h2");
			//check for h3
			addTag(doc,"h3");
			
			//adding positions
			String text=Jsoup.parse(html).text().toLowerCase(); 
			//System.out.println(text);
			addTagPositions(text,"pos");
			//check for url
			
			
	    
				
		}  catch (UnsupportedEncodingException e) {
			e.printStackTrace();
			return everything1;		
			
		} catch (MalformedURLException e) {
			e.printStackTrace();
			return everything1;	
			
		} 
		catch(Exception e){
			e.printStackTrace();
			return everything1;	
		}
		
		return everything1;
		

	}
	
	public  void addTag(Document doc,String tag){
		int tagSize = doc.select(tag).size();
		////System.out.println(tagSize+"tagSize");
		HashMap<String, ArrayList<Integer>> value=null;
		ArrayList<Integer> v=null;
		 Matcher matcher = null;
		for(int i=0; i<tagSize; i++)
		{	
			Element tags = doc.select(tag).get(i);
			String tagText = tags.text();
			////System.out.println(tagText+"tagText");
			StringTokenizer st2=new StringTokenizer(tagText,".,-?' \"\\’~#!@$%^&*_=+(){}[]<>;:/`\t\n|\u00A0");
			while (st2.hasMoreElements()) {
				String word=st2.nextElement().toString().trim().toLowerCase();
				 matcher = pattern.matcher(word);
				if(!ar.contains(word)&& matcher.find() && matcher.group().length()==word.length()){
				//if(!word.equals("www")&&!word.equals("http")&&!word.equals("com")&&!word.equals("edu")&&!word.equals("net")&&!word.equals("org")){
					word=MorphaStemmer.morpha(word, false);

					value = new HashMap<String, ArrayList<Integer>>();
				 v=new ArrayList<Integer>();
				

				if(everything1.get(word)!=null)
				{
					value=everything1.get(word);
					
					if(value.get(tag) != null)
					{
						v= value.get(tag);
						v.add(1);
						value.put(tag, v);
					}
					else
					{	v.add(1);
						value.put(tag, v);
					}
					everything1.put(word, value);
				}
				else
				{	v.add(1);
					value.put(tag, v);
					everything1.put(word, value);
				}
			}
			}
		}

	}
	public  void addTagPositions(String text, String tag){
		StringTokenizer st3=new StringTokenizer(text,".,-?' \"\\’~#!@$%^&*_=+(){}[]<>;:/`\t\n|\u00A0");
    	int count=0;
    	HashMap<String, ArrayList<Integer>> value=null;
    	ArrayList<Integer> position=null;
		 Matcher matcher = null;

		while (st3.hasMoreElements()) {
			String word=st3.nextElement().toString().trim().toLowerCase();

			 matcher = pattern.matcher(word);
			count++;
			/*if(word.contains("islands")){
				//System.out.println(word+"hello"+count);
			word=word.trim();
			word=word.replace("\u00A0", "");
			//System.out.println(word+"after"+count);
			}*/
			if(!ar.contains(word) && matcher.find() && matcher.group().length()==word.length()){
				word=MorphaStemmer.morpha(word, false);

				 value = new HashMap<String, ArrayList<Integer>>();
				 position=new ArrayList<Integer>();
				int maxCount=0;
				if(everything1.get(word)!=null)
					
				{
					value=everything1.get(word);
					if(value.get(tag) != null)
					{
						position= value.get(tag);
						position.add(count);
						value.put(tag, position);
					}
					else
					{	position.add(count);
						value.put(tag, position);
					}
					everything1.put(word, value);
				}
				else
				{	position.add(count);
					value.put(tag, position);
					everything1.put(word, value);
				}
				if(	maxCountMap.size()!=0 && maxCountMap.get(word)!=null){
					maxCount=maxCountMap.get(word);
					maxCount+=1;
					maxCountMap.put(word, maxCount);
					////System.out.println("i exist adding for word "+word+" count "+maxCount);

				}
				else{
					//maxCount=1;
					////System.out.println("i dont exist yet adding 1 for word "+word);
					maxCountMap.put(word, 1);
				}
				
			}
			
		}
		////System.out.println(count);
		/*HashMap<String, ArrayList<Integer>> countMap = new HashMap<String, ArrayList<Integer>>();
		ArrayList<Integer> counts=new ArrayList<Integer>();
		counts.add(count);
		countMap.put("totalWords", counts);
		everything1.put("totalWords", countMap);*/
	}
	
	public  void addURLHostPositions(String text,  String tag){
		StringTokenizer st3=new StringTokenizer(text,".,-?' \"\\’~#!@$%^&*_=+(){}[]<>;:/`\t\n|\u00A0");
    	int count=0;
    	HashMap<String, ArrayList<Integer>> value =null;
    	ArrayList<Integer> position=null;
		 Matcher matcher = null;

		while (st3.hasMoreElements()) {
			String word=st3.nextElement().toString().trim().toLowerCase();

			 matcher = pattern.matcher(word);

			count++;
			if(!ar.contains(word) && matcher.find() && matcher.group().length()==word.length()){
				if(!word.equals("www")&&!word.equals("http")&&!word.equals("com")&&!word.equals("edu")&&!word.equals("net")&&!word.equals("org")){
					word=MorphaStemmer.morpha(word, false);

				 value = new HashMap<String, ArrayList<Integer>>();
				 position=new ArrayList<Integer>();
				
				if(everything1.get(word)!=null)
					
				{
					value=everything1.get(word);
					if(value.get(tag) != null)
					{
						position= value.get(tag);
						position.add(count);
						value.put(tag, position);
					}
					else
					{	position.add(count);
						value.put(tag, position);
					}
					everything1.put(word, value);
				}
				else
				{	position.add(count);
					value.put(tag, position);
					everything1.put(word, value);
				}
			}
			}
		}
	}


}
