package org.apache.hadoop.mapred;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class JobStateChangeMonitor extends JobInProgressListener {

	private static final Log LOG = LogFactory.getLog(JobStateChangeMonitor.class);
	
	@Override
	public void jobAdded(JobInProgress job) throws IOException {
		System.out.println("Job Added Event");
		LOG.info("Job Added Event");
	}

	@Override
	public void jobRemoved(JobInProgress job) {
		System.out.println("Job Removed Event");
		LOG.info("Job Removed Event");
	}

	@Override
	public void jobUpdated(JobChangeEvent event) {
		System.out.println("Job Event");
		LOG.info("Job Event");
		if (event.getJobInProgress().getStatus().getRunState() ==
				JobStatus.SUCCEEDED) {
			System.out.println("Job Completed");
			LOG.info("DRON_ID: " + event.getJobInProgress().getDronJobInstanceId());
			System.out.println("DRON_ID: " +
				event.getJobInProgress().getDronJobInstanceId());
		}
	}

}
