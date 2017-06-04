package upem.jarret.job;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Task {
	private final long jobId;
	private String workerVersion;
	private String workerURL;
	private String workerClassName;
	private int taskNumber;

	public void setTask(int task) {
		this.taskNumber = task;
	}

	public Task(long jobId, String workerVersionNumber, String workerURL, String workerClassName, int taskNumber) {
		this.jobId = jobId;
		this.workerVersion = workerVersionNumber;
		this.workerURL = workerURL;
		this.workerClassName = workerClassName;
		this.taskNumber = taskNumber;
	}

	@JsonProperty("JobId")
	public long getJobId() {
		return jobId;
	}

	@JsonProperty("WorkerVersionNumber")
	public String getWorkerVersionNumber() {
		return workerVersion;
	}

	@JsonProperty("WorkerURL")
	public String getWorkerURL() {
		return workerURL;
	}

	@JsonProperty("WorkerClassName")
	public String getWorkerClassName() {
		return workerClassName;
	}

	@JsonProperty("Task")
	public int getJobTaskNumber() {
		return taskNumber;
	}

}