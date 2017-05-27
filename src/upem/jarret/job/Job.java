package upem.jarret.job;

import java.util.BitSet;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Job {

	private long jobId;
	private int jobTaskNumber;
	private String jobDescription;
	private int jobPriority;
	private double workerVersionNumber;
	private String workerURL;
	private String workerClassName;
	private BitSet bitSetTasks = new BitSet(this.jobTaskNumber);;
	private int doneTasksNumber;

	public Job(String jobId, String jobTaskNumber, String jobDescription, String jobPriority,
			String workerVersionNumber, String workerURL, String workerClassName) {
		this.jobId = Long.parseLong(jobId);
		this.jobTaskNumber = Integer.parseInt(jobTaskNumber);
		this.jobDescription = jobDescription;
		this.jobPriority = Integer.parseInt(jobPriority);
		this.workerVersionNumber = Double.parseDouble(workerVersionNumber);
		this.workerURL = workerURL;
		this.workerClassName = workerClassName;
	}

	public Job() {
	}

	public BitSet getBitSet() {
		return bitSetTasks;
	}

	@JsonProperty("JobId")
	public long getJobId() {
		return jobId;
	}

	@JsonProperty("JobTaskNumber")
	public int getJobTaskNumber() {
		return jobTaskNumber;
	}

	@JsonProperty("JobDescription")
	public String getJobDescription() {
		return jobDescription;
	}

	@JsonProperty("JobPriority")
	public int getJobPriority() {
		return jobPriority;
	}

	@JsonProperty("WorkerVersionNumber")
	public double getWorkerVersionNumber() {
		return workerVersionNumber;
	}

	@JsonProperty("WorkerURL")
	public String getWorkerURL() {
		return workerURL;
	}

	@JsonProperty("WorkerClassName")
	public String getWorkerClassName() {
		return workerClassName;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(jobId).append(System.getProperty("line.separator"));
		sb.append(jobTaskNumber).append(System.getProperty("line.separator"));
		sb.append(workerURL).append(System.getProperty("line.separator"));
		return sb.toString();
	}

	public void incrementeJobTaskNumber() {
		this.jobTaskNumber++;
	}
	
	public int getIndexOfAvailableTask() {
		return bitSetTasks.nextClearBit(0);
	}

}
