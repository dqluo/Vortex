package edu.upenn.cis455.imageIndexer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import edu.upenn.cis455.S3.S3Access;

public class IndexMapper extends Mapper<Text,Text,Text,Text>{

	public void map(Text key, Text value, Context context) throws IOException, InterruptedException

	{
		
	try{
		//if(key!=null && value!=null)
		
	String keyscheck=key.toString();
	String valuescheck=value.toString();
		if(keyscheck!=null && keyscheck.length()!=0 && valuescheck.length()!=0 && valuescheck!=null && !valuescheck.equalsIgnoreCase("null") && !keyscheck.equalsIgnoreCase("null")){
		Text wordText = new Text();
		Text keyText = new Text();
		
		String sep="<*&SEPARATOR&*>";
		String b=value.toString().trim();
		if(b.contains("<*&SEPARATOR&*>")){
		int index=b.indexOf(sep); 
		
		String val2=b.substring(0,index);
		String val1=b.substring(val2.length()+sep.length());

		if (val1!=null && val2!=null && !val1.equals("null") && !val2.equals("null")
				&& !key.toString().equals("0")   ) {
			ExtractTags et=new ExtractTags();
		HashMap<String, HashMap<String, ArrayList<Integer>>> hm = et.getTags(key.toString(), val1,val2);
		if(hm!=null && hm.size()!=0){
		int maxCount = 0;
		HashMap<String, Integer> maxCountMap=et.maxCountMap;
		if(maxCountMap.values()!=null && maxCountMap.values().size()!=0){
			SortedSet<Integer> values = new TreeSet<Integer>(maxCountMap.values());
			if(values!=null && values.size() !=0){
				maxCount=values.last();
			}
			}
		S3Access s3=new S3Access();
		ArrayList<String> dict=s3.getDictionary();
		
		////System.out.println(dict);
		//printing map with positions    	
		//HashMap<String, ArrayList<Integer>> countList=	hm.get("totalWords");
		//int count=countList.get("totalWords").get(0);//if nothing should be zero
		String out= "";
		for(String hmkey : hm.keySet())
		{	
			out= ""+key;
			int boldCount=0;
			int italicCount=0;
			int h1Count=0;
			int h2Count=0;
			int h3Count=0;
			int linkCount=0;
			String pos="<";
			String title="<";
			String url="<";
			String host="<";
			int tfNum=0;
			double tf=0.00;
			int hostFlag=0;
			int urlFlag=0;
			int titleFlag=0;
			//int english=0;
			HashMap<String, ArrayList<Integer>> hmvalue = new HashMap<String, ArrayList<Integer>>();
			hmvalue=hm.get(hmkey);
			for(String innerKey : hmvalue.keySet())
			{	
				if(innerKey.equals("b"))	{
					boldCount+=hmvalue.get(innerKey).size();
				}
				else if(innerKey.equals("i"))	{
					italicCount+=hmvalue.get(innerKey).size();
				}
				else if(innerKey.equals("a"))	{
					linkCount+=hmvalue.get(innerKey).size();
				}
				else if(innerKey.equals("h1"))	{
					h1Count+=hmvalue.get(innerKey).size();
				}
				else if(innerKey.equals("h2"))	{
					h2Count+=hmvalue.get(innerKey).size();
				}
				else if(innerKey.equals("h3"))	{
					h3Count+=hmvalue.get(innerKey).size();
				}
				else if(innerKey.equals("pos"))	{

					for(Integer a:hmvalue.get(innerKey)){
						pos+=a+";";
						tfNum++;
					}
					pos=pos.substring(0, pos.length()-1);
					pos+=">";

				}
				else if(innerKey.equals("title"))	{

					for(Integer a:hmvalue.get(innerKey)){
						title+=a+";";
						titleFlag=1;
						//tfNum++;
					}
					title=title.substring(0, title.length()-1);
					title+=">";

				}
				else if(innerKey.equals("url"))	{

					for(Integer a:hmvalue.get(innerKey)){
						url+=a+";";
						//tfNum++;
						urlFlag=1;
						
					}
					url=url.substring(0, url.length()-1);
					url+=">";

				}
				else if(innerKey.equals("host"))	{

					for(Integer a:hmvalue.get(innerKey)){
						host+=a+";";
						hostFlag=1;
						//tfNum++;
					}
					host=host.substring(0, host.length()-1);
					host+=">";

				}

			}
			if(title.equals("<")){
				title+="0>";
			}
			if(pos.equals("<")){
				pos+="0>";
			}
			if(host.equals("<")){
				host+="0>";
			}
			if(url.equals("<")){
				url+="0>";
			}

			if (maxCount != 0) {
				//System.out.println(tfNum+ "tfNum");
				//System.out.println(maxCount + "count");
				tf =0.5+0.5*( (double) tfNum / (double) maxCount);
			}
			if(dict==null || dict.size()==0){
				//System.out.println("yes I read from dictionary of size"+dict.size());
				out+="=b:"+boldCount+",i:"+italicCount+",a:"+linkCount+",h1:"+h1Count+",h2:"+h2Count+",h3:"+h3Count+",pos:"+pos+",host:"+host+",url:"+url+",title:"+title+",tf:"+tf+"<@!Separatorurl!@>"+val2+"<*LEXSEPARATOR*>1";

			}
			else{
				if(dict.contains(hmkey)){
					out+="=b:"+boldCount+",i:"+italicCount+",a:"+linkCount+",h1:"+h1Count+",h2:"+h2Count+",h3:"+h3Count+",pos:"+pos+",host:"+host+",url:"+url+",title:"+title+",tf:"+tf+"<@!Separatorurl!@>"+val2+"<*LEXSEPARATOR*>1";
					//System.out.println("dict containe me" +hmkey+" "+out);
				}
				else{
					if( hostFlag==1 || urlFlag ==1 || titleFlag==1){
					out+="=b:"+boldCount+",i:"+italicCount+",a:"+linkCount+",h1:"+h1Count+",h2:"+h2Count+",h3:"+h3Count+",pos:"+pos+",host:"+host+",url:"+url+",title:"+title+",tf:"+tf+"<@!Separatorurl!@>"+val2+"<*LEXSEPARATOR*>1";
					//System.out.println("adding to dict" +hmkey+" "+out);
					}
					else{
						out+="=b:"+boldCount+",i:"+italicCount+",a:"+linkCount+",h1:"+h1Count+",h2:"+h2Count+",h3:"+h3Count+",pos:"+pos+",host:"+host+",url:"+url+",title:"+title+",tf:"+tf+"<@!Separatorurl!@>"+val2+"<*LEXSEPARATOR*>0";
						//System.out.println("not adding to dict" +hmkey+" "+out);

					}
				}
			}
			wordText.set(out);
			keyText.set(hmkey);
			context.write(keyText, wordText);
		}
		}
	}
	}
	}
	}catch(Exception e){
		//System.out.println("");
		e.printStackTrace();
	}
	}
}
