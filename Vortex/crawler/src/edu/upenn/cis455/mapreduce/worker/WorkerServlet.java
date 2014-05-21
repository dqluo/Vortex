package edu.upenn.cis455.mapreduce.worker;


import javax.servlet.*;
import javax.servlet.http.*;

public class WorkerServlet extends HttpServlet 
{

  static final long serialVlersionUID = 455555002;
  String master;
  String masterhost;
  int masterport;
  int workerport;
  String status = "idle";
  public void doGet(HttpServletRequest request, HttpServletResponse response) 
       throws java.io.IOException
  {


  }
  public void init() throws ServletException
  {
	  	Worker.storagedir = getInitParameter("storagedir");
	    String wp = getInitParameter("workerport");
	    Worker.workerIPport = wp;
		String mp = getInitParameter("master");
		
		if(mp!=null)
		{
		    String splitHostAndPort[] =  mp.split(":");
		    Worker.masterhost = splitHostAndPort[0];
		    Worker.masterport = Integer.valueOf( splitHostAndPort[1]);
		}
		if(wp!=null)
		Worker.workerport = Integer.valueOf(wp.split(":")[1]);
	    Thread one  =new Thread() 
	    {

		    public void run()
		    {

				while(true)
				{
					  if(Worker.shouldStop)
						  Worker.status = "stopped";
                    if(!Worker.status.equals("waiting"))
                        WorkerStatus.reportWorkerStatus(Worker.masterhost, Worker.masterport , Worker.workerIPport, Worker.status,Crawler.globalCount);
				    try
				    {				    	
						Thread.sleep(10000);
					} 
				    catch (InterruptedException e) 
					{
						e.printStackTrace();
					}
				}
		    } 
		};one.start();
	  }


}
  
