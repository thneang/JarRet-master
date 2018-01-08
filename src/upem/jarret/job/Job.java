package upem.jarret.job;

import java.io.IOException;
import java.util.Objects;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

public class Job {
	private final String jobId;
	private final String jobTaskNumber;
	private final String jobDescription;
	private final String jobPriority;
	private final String workerVersion;
	private final String workerURL;
	private final String workerClassName;
	private int currentTask;
	private boolean isFinished = false;

	private Job(String jobId, String jobTaskNumber, String jobDescription, String jobPriority, String workerVersion,
	        String workerURL, String workerClassName, int currentTask) {
		this.jobId = Objects.requireNonNull(jobId);
		this.jobTaskNumber = Objects.requireNonNull(jobTaskNumber);
		this.jobDescription = Objects.requireNonNull(jobDescription);
		this.jobPriority = Objects.requireNonNull(jobPriority);
		this.workerVersion = Objects.requireNonNull(workerVersion);
		this.workerURL = Objects.requireNonNull(workerURL);
		this.workerClassName = Objects.requireNonNull(workerClassName);
		this.currentTask = Objects.requireNonNull(currentTask);
	}

	/**
	 * Functions used by the JSON parser
	 */
	
	public int getCurrentTask() {
		return currentTask;
	}

	public String getJobId() {
		return jobId;
	}

	public String getJobTaskNumber() {
		return jobTaskNumber;
	}

	public String getJobDescription() {
		return jobDescription;
	}

	public String getJobPriority() {
		return jobPriority;
	}

	public String getWorkerVersion() {
		return workerVersion;
	}

	public String getWorkerURL() {
		return workerURL;
	}

	public String getWorkerClassName() {
		return workerClassName;
	}
	
	public boolean isFinished() {
	    return isFinished;
    }

	/**
	 * Parses a json object to get the job
	 * 
	 * @param jp
	 * @return
	 * @throws JsonParseException
	 * @throws IOException
	 */
	public static Job parseJSON(JsonParser jp) throws JsonParseException, IOException {
	
		String jobId = null;
		String jobTaskNumber = null;
		String jobDescription = null;
		String jobPriority = null;
		String workerVersion = null;
		String workerURL = null;
		String workerClassName = null;

		while (jp.nextToken() != JsonToken.END_OBJECT) {
			String fieldName = jp.getCurrentName();
			jp.nextToken();
			switch (fieldName) {
			case "JobId":
				jobId = jp.getText();
				break;
			case "JobTaskNumber":
				jobTaskNumber = jp.getText();
				break;
			case "JobDescription":
				jobDescription = jp.getText();
				break;
			case "JobPriority":
				jobPriority = jp.getText();
				break;
			case "WorkerVersionNumber":
				workerVersion = jp.getText();
				break;
			case "WorkerURL":
				workerURL = jp.getText();
				break;
			case "WorkerClassName":
				workerClassName = jp.getText();
				break;
			default:
				System.err.println("Unknown Field");
			}
		}

		return new Job(jobId, jobTaskNumber, jobDescription, jobPriority, workerVersion, workerURL, workerClassName, 0);
	}

	/**
	 * Creates the next task of the job
	 * 
	 * @return
	 * @throws IOException
	 */
	public Task nextTask() throws IOException {
		Task task = new Task(jobId, workerVersion, workerURL, workerClassName, currentTask);
		updateCurrentTask();
		return task;
	}
	
	/**
	 * Increments currentTask or set isFinished to true if all the tasks are done
	 */
	private void updateCurrentTask() {
	    currentTask++;
	    if(currentTask > Integer.parseInt(jobTaskNumber)) {
	    	isFinished = true;
	    }
    }

	@Override
    public String toString() {
	    return "Job [jobId=" + jobId + ", jobTaskNumber=" + jobTaskNumber + ", jobDescription=" + jobDescription
	            + ", jobPriority=" + jobPriority + ", workerVersion=" + workerVersion + ", workerURL=" + workerURL
	            + ", workerClassName=" + workerClassName + ", currentTask=" + currentTask + "]";
    }
		
}
