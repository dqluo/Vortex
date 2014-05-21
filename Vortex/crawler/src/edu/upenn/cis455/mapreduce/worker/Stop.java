package edu.upenn.cis455.mapreduce.worker;



import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class Stop extends HttpServlet 
{
	private static final long serialVersionUID = -7612836948975040347L;
	public StringBuffer completebody=new StringBuffer();
	public void doGet(HttpServletRequest request, HttpServletResponse response) throws java.io.IOException
	{
		Worker.shouldStop =  true;
		Worker.mapStarted = false;
	}
}

