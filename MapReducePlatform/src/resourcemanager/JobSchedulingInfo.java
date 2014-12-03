package resourcemanager;

import java.util.ArrayList;
import info.JobConf;

class JobsSchedulingInfo{
	public ArrayList<JobConf> canceledJobs = new ArrayList<JobConf>();
	public ArrayList<JobConf> newJobs = new ArrayList<JobConf>();
	public ArrayList<JobConf> prioritizedNewJobs = new ArrayList<JobConf>();
}
