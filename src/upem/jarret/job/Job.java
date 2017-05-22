package upem.jarret.job;

public class Job {

	private final String JobId;
	private final String JobTaskNumber;
	private final String JobDescription;
	private final String JobPriority;
	private final String WorkerVersionNumber;
	private final String WorkerURL;
	private final String WorkerClassName;

	public Job(String jobId, String jobTaskNumber, String jobDescription, String jobPriority,
			String workerVersionNumber, String workerURL, String workerClassName) {
		super();
		JobId = jobId;
		JobTaskNumber = jobTaskNumber;
		JobDescription = jobDescription;
		JobPriority = jobPriority;
		WorkerVersionNumber = workerVersionNumber;
		WorkerURL = workerURL;
		WorkerClassName = workerClassName;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(JobId).append(System.getProperty("line.separator"));
		sb.append(JobTaskNumber).append(System.getProperty("line.separator"));
		sb.append(WorkerURL).append(System.getProperty("line.separator"));
		return sb.toString();
	}
}
