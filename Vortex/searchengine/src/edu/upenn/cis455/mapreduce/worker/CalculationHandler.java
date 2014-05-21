package edu.upenn.cis455.mapreduce.worker;

import edu.upenn.cis455.mapreduce.AllStrings;
import edu.upenn.cis455.mapreduce.storage.RankerStore;

public class CalculationHandler extends Thread{
	public RankerStore rankerstore;
	public CalculationHandler(RankerStore rankerstore)
	{
		this.rankerstore = rankerstore;
	}
	public void run()
	{
		String docs="";
		while((docs=GetDocs.getADocument()) != null)
		{
			StringBuilder valueToReturn = new StringBuilder();
			double total= 0.0;
			double tf_idf = 0.0;
			String positionField="";
			//If data is not sanitize
			String dataAndUrl[] = docs.split(AllStrings.urlSeperator);
			String docID = dataAndUrl[0].split("=")[0].trim();
			String forCalc = dataAndUrl[0].split("=")[1].trim();
			valueToReturn.append(AllStrings.reduceSeperator);
			valueToReturn.append(docID);
			valueToReturn.append(AllStrings.newSeperator);
			valueToReturn.append(dataAndUrl[1].trim());
			
			//System.out.println(dataAndUrl[1]);
			
			String fields[] = forCalc.split(",");
			for(int j=0; j<fields.length; j++)
			{
				//System.out.println("\t"+fields[j]);
				String a = fields[j].split(":")[1].trim();
				//Checking if the word came in title host or url
				if(fields[j].contains("title") || fields[j].contains("host") || fields[j].contains("url"))
				{
					//If the metric value is zero then do not do anything
					if(a.length() == 3 && a.charAt(1)=='0')
						continue;
					else
					{
						if(a.contains(";"))
						{
							String splitA[] = a.split(";");
							total = total + ((splitA.length) * 25);
						}
						else
							total += 20;
					}
						
				}
				
				else if(fields[j].contains("a") || fields[j].contains("h2") || 
						fields[j].contains("h3") || fields[j].contains("i"))
				{
					if(a.charAt(0)=='0')
						continue;
					else
					{
						total=total+(Double.parseDouble(a.substring(0,a.length())) * 10);
					}
						
				}
				
				else if(fields[j].contains("b") || fields[j].contains("h1"))
				{
					if(a.charAt(0)=='0')
						continue;
					else
						total=total+ (Double.parseDouble(a.substring(0,a.length())) * 15);
				}
				
				else if(fields[j].contains("tf"))
				{
					tf_idf = Double.parseDouble(fields[j].split(":")[1].trim()) * GetDocs.idf;
				}
				else if(fields[j].contains("pos"))
				{
					positionField = fields[j];
				}
					
			}
			
			double pageRank=Worker.getPageRank(docID, total,rankerstore);
			
			//System.out.println("\tidf: "+GetDocs.idf);
			//System.out.println("\ttf_idf: "+tf_idf);
			total += (pageRank * 30);
			total = total * tf_idf;
			
			//System.out.println("\tFinal ranks are: "+total);
			
			valueToReturn.append(AllStrings.urlSeperator);
			valueToReturn.append(positionField);
			valueToReturn.append(AllStrings.urlSeperator);
        	valueToReturn.append(String.valueOf(tf_idf));
			
        	synchronized(Worker.valuesAfterRank)
        	{
        		Worker.valuesAfterRank.put(valueToReturn.toString(), total);
        	}
        	
				
		
		}
	}
	

}
