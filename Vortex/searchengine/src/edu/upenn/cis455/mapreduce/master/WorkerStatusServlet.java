package edu.upenn.cis455.mapreduce.master;

import javax.servlet.http.*;

import edu.upenn.cis455.mapreduce.AllStrings;

public class WorkerStatusServlet extends HttpServlet {

  static final long serialVersionUID = 455555001;
  static boolean reduceCheck = false;
  
  public void doGet(HttpServletRequest request, HttpServletResponse response) 
       throws java.io.IOException
  {
    response.setContentType("text/html");
    //Get all values from the request parameters
    String[] IPPortSplit = request.getParameter(AllStrings.portString).split(":");
    int workerPort = Integer.valueOf(IPPortSplit[1]);
    String workerhost = IPPortSplit[0];
    String status = request.getParameter(AllStrings.statusString);
 
    Master.updateWorkerStatus(workerPort,workerhost, status);
    Master.setActiveWorkers();

    
    
  }
}
  
