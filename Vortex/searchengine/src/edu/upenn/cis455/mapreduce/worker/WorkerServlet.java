package edu.upenn.cis455.mapreduce.worker;


import javax.servlet.*;
import javax.servlet.http.*;

import edu.upenn.cis455.mapreduce.master.Master;

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
		Worker.me = Master.getWorkerNumber(Worker.workerport);
		//Worker.databasestore ="/home/ec2-user/worker"+Worker.me+"/Input/IndexStore";
	    Worker.indexstore = "/home/ec2-user/worker"+Worker.me+"/Input/IndexStore";
	    Worker.rankstore = "/home/ec2-user/worker"+Worker.me+"/Input/RankStore";
		//Worker.indexstore = "/home/ec2-user/master/IndexStore"+Worker.me;
		//Worker.rankstore = "/home/ec2-user/master/RankStore"+Worker.me;
		Thread one  =new Thread() 
	    {

		    public void run()
		    {

				while(true)
				{
                    WorkerStatus.reportWorkerStatus(Worker.masterhost, Worker.masterport , Worker.workerIPport, Worker.status);
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
  
