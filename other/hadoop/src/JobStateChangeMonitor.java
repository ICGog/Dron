package org.apache.hadoop.mapred;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.dron.Dron;
import org.json.simple.JSONObject;

public class JobStateChangeMonitor extends JobInProgressListener {

	private static final Log LOG = LogFactory.getLog(JobStateChangeMonitor.class);
	
	private final Dron dron;
	private final String exchangeName;
	
	public JobStateChangeMonitor(Dron dron, String exchangeName) {
		this.dron = dron;
		this.exchangeName = exchangeName;
	}
	
	@Override
	public void jobAdded(JobInProgress job) throws IOException {
		// Ignore Event.
	}

	@Override
	public void jobRemoved(JobInProgress job) {
		// Ignore Event.
	}

	@Override
	public void jobUpdated(JobChangeEvent event) {
		JobInProgress jobInProgress = event.getJobInProgress();
		if (jobInProgress.getStatus().getRunState() == JobStatus.SUCCEEDED) {		
			JSONObject jsonObject = new JSONObject();
			jsonObject.put("job_instance",
					jobInProgress.getJobConf().get("dronJobInstanceId", "-1"));
			jsonObject.put("state", "succeeded");
			publish(jsonObject.toJSONString());
		} else if (jobInProgress.getStatus().getRunState() == JobStatus.FAILED) {
			JSONObject jsonObject = new JSONObject();
			jsonObject.put("job_instance",
					jobInProgress.getJobConf().get("dronJobInstanceId", "-1"));
			jsonObject.put("state", "failed");
			publish(jsonObject.toJSONString());			
		} else {
			// TODO(ionel): handle other job statuses.
			LOG.info("Dron job instance " + 
					jobInProgress.getJobConf().get("dronJobInstanceId", "-1") +
					" is in state " + jobInProgress.getStatus().getRunState());
		}
	}

	private void publish(String message) {
		LOG.info("Publishing " + message);
		try {
			dron.publish(exchangeName, message);
		} catch (IOException e) {
			LOG.error("Could not publish " + message, e);
		}
	}

}
