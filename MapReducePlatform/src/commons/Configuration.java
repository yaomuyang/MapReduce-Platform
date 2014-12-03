package commons;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/*
 * This serves as a general configuration components for all the components to use
 */
@SuppressWarnings("serial")
public class Configuration extends Properties {	
	
	public Configuration() throws IOException{
		//set default configuration, this is useful for some optional configurations
		this.setDefaultConfiguration();
		//set configuration according to the configuration file
		this.setConfiguration();
	}	
	
	private void setConfiguration() throws IOException{		
		//read the configuration file
		//String confPath = System.getenv("MAPREDUCE_CONF_DIR") + "mapreduce.conf";
		String confPath = "mapreduce.conf";
		FileInputStream fis = new FileInputStream(confPath);
		this.load(fis);
		fis.close();
	}
	
	/*
	 * We use this function to set all those default configurations
	 * It is okay that configuration items are not set in the conf file, we'll just use the default value
	 */
	private void setDefaultConfiguration(){
		this.setProperty("DefaultNodeCapacity", "5");
		this.setProperty("ReplicaNumber", "3");
		this.setProperty("JobReattemptTimes", "2");
		this.setProperty("ReducerCount", "3");
	}
}