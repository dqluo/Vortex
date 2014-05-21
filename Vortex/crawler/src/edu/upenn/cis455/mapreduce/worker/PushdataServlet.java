package edu.upenn.cis455.mapreduce.worker;

import java.io.BufferedReader;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class PushdataServlet extends HttpServlet 
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 401488683032300340L;

	public void doPost(HttpServletRequest request, HttpServletResponse response) throws java.io.IOException
	{
			BufferedReader in=request.getReader();
			StringBuffer completebody=new StringBuffer();
			String body;
            synchronized(Worker.pushedData){

                while((body = in.readLine()) != null)
                {
		              Worker.pushedData.append(body);
		              Worker.pushedData.append("\n");
		    
                    //Worker.pushedData.append(completebody);
                    Worker.noOfWorkersPushed++;
                }
            }
			  
    }
}


