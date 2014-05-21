package edu.upenn.cis455.mapreduce;


import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import edu.upenn.cis455.mapreduce.worker.Worker;

public class WorkerContext implements Context {


	public static int checkAndRedirectRange(String key)
	{
		String high = "ffffffffffffffffffffffffffffffffffffffff";
		BigInteger highrange = new BigInteger(high,16);
		String hexString = convertToSHAHash(key);
		BigInteger keyBigInt = new BigInteger(hexString,16);
		BigInteger divideFactor =highrange.divide(BigInteger.valueOf(Worker.noOfWorkers));

		for(int i=1;i<=Worker.noOfWorkers;i++)
		{
			if(keyBigInt.compareTo(divideFactor.multiply(BigInteger.valueOf(i))) == -1 
					||keyBigInt.compareTo(divideFactor.multiply(BigInteger.valueOf(i))) == 0 )
			{
				return i;
				
			}
		}		
		
		
		return Worker.noOfWorkers;
	}
	public static String convertToSHAHash(String key)
	{
		MessageDigest md;
		 StringBuffer sb = new StringBuffer();
		 try
		 {
			md = MessageDigest.getInstance("SHA-1");
	        md.update(key.getBytes());
	        byte byteData[] = md.digest();
	        for (int i = 0; i < byteData.length; i++) 
	        {
	        	sb.append(Integer.toString((byteData[i] & 0xff) + 0x100, 16).substring(1));
	        }
		  } 
		 catch (NoSuchAlgorithmException e) 
		 {
			System.out.println("Exception while converting into SHA-1");
		 }
	        return sb.toString();

	}
	

	synchronized public static void writeToFile(File file,String content)
	{
		FileWriter fw;

		try 
		{			
			{
				fw = new FileWriter(file.getAbsoluteFile(),true);
				synchronized(fw)
				{
				
				PrintWriter pw = new PrintWriter(fw);
				pw.write(content);
				pw.write("\n");
				pw.close();
				fw.close();
				}
			}
		}
		catch (IOException e) 
		{
			System.out.println("I/O Exception while writing to file from runmap");
		}
		}
	@Override
	public void write(String key, String value) {
		// TODO Auto-generated method stub
		
	}
	
	
}
