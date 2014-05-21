package edu.upenn.cis455.mapreduce.master;

public class WorkerInMaster {

	
	String status;
	int keysRead;
	int keysWritten;
	long lastEdited;
	boolean isActive = false;
	String workerhost;
    public int seedcount = 1;
    public int creationcount = 2;
    public int lineCountOfBFR = 0;
	
	public void setWorkerHost(String workerhost)
	{
		this.workerhost = workerhost;
	}
	public void setStatus(String status)
	{
		this.status = status;
	}
	//TODO remove this before uploading
	public void setKeysRead(int keysRead)
	{
		this.keysRead = keysRead;
	}
	public void setKeysWritten(int keysWritten)
	{
		this.keysWritten= keysWritten;
	}
	public void setLastEdited()
	{
		this.lastEdited = System.currentTimeMillis();
	}
	public void setIsActive(boolean isActive)
	{
		this.isActive = isActive;
	}
	public String getStatus()
	{
		return status;
	}
	public int getKeysRead()
	{
		return keysRead;
	}
	public int getKeysWritten()
	{
		return keysWritten;
	}
	public boolean getIsActive()
	{
		return isActive;
	}
	public long getLastEdited()
	{
		return lastEdited;
	}
	public String getWorkerHost()
	{
		return workerhost;
	}
	
}
