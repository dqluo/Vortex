package edu.upenn.cis455.lexicon;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import edu.upenn.cis455.S3.S3Access;

public class LexiconMapper extends Mapper<Text,Text,Text,Text>{

	public void map(Text key, Text value, Context context) throws IOException, InterruptedException

	{
		S3Access s3=new S3Access();
		ArrayList<String> dict=s3.getDictionary();
		
		Text wordText = new Text();
		Text keyText = new Text();
		
		
		MessageDigest md;
		StringBuilder sb=null;
		for(String word:dict){
		 sb=new StringBuilder();

		try {
			md = MessageDigest.getInstance("SHA1");
		
		byte[] mdbytes = md.digest(word.getBytes());
		
        for (int i1 = 0; i1 < mdbytes.length; i1++) {
          sb.append(Integer.toString((mdbytes[i1] & 0xff) + 0x100, 16).substring(1));
        }
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		String x=sb.toString();
		wordText.set(word);
		keyText.set(x);
		//System.out.println(x+ " "+word);
		context.write(keyText, wordText);
	}
	
		
		
	}
}