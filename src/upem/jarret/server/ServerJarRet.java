package upem.jarret.server;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import upem.jarret.job.Job;
import utils.JsonUtils;

public class ServerJarRet {

	static class Context {
		private HTTPServerReader reader;
		private boolean readingRequest = true;
		private boolean requestingTask = false;
		private boolean sendingPost = false;
		private boolean parsingRequest = false;
		private boolean readingAnswer = false;
		private boolean inputClosed = false;
		private String request;
		private String response;
		private int contentLength;
		private final ByteBuffer in = ByteBuffer.allocate(BUF_SIZE);
		private final ByteBuffer out = ByteBuffer.allocate(BUF_SIZE);
		private final SelectionKey key;
		private final SocketChannel sc;
		private long inactiveTime;

		/**
		 * Context constructor
		 * 
		 * @param key
		 *            SelectionKey given by the server
		 * @throws IOException
		 *             in case of problems in reading from socket channel
		 */
		public Context(SelectionKey key) throws IOException {
			this.key = key;
			this.sc = (SocketChannel) key.channel();
			this.reader = new HTTPServerReader(sc, in);
		}

		/**
		 * Treats the received data after being called by the selector
		 * 
		 * @throws IOException
		 *             in case of problems in reading from socket channel
		 */
		public void doRead() throws IOException {
			if (sc.read(in) == -1) {
				inputClosed = true;
			}
			resetInactiveTime();
			process();
			updateInterestOps();
		}

		/**
		 * Parses the received request and performs the needed instructions to
		 * send responses to client
		 * 
		 * @throws IOException
		 *             in case of problems in reading from socket channel
		 */
		private void process() throws IOException {
			if (isReadingRequest()) {
				try {
					setRequest(reader.readLineCRLF());
					System.out.println("current request : \n" + getRequest());
					setReadingRequest(false);
				} catch (IllegalStateException e) {
					// Pour dire que la lecture de la ligne n'a rien donné
					return;
				}
			}
			try {
				parseRequest(sc);
			} catch (IllegalStateException e) {
				out.put(CHARSET_UTF_8.encode(BAD_REQUEST));
			}
			if (isRequestingTask()) {
				setRequestingTask(false);
				out.put(ServerJarRet.getAvailableTask((SocketChannel) key.channel()));
			} else if (isSendingPost()) {
				if (response == null) {
					throw new IllegalArgumentException("No answer");
				}
				if (JsonUtils.isValidJsonString(response)) {
					out.put(CHARSET_UTF_8.encode(HTTP_1_1_200_OK));
				} else {
					out.put(CHARSET_UTF_8.encode(BAD_REQUEST));
				}
				reset(sc);
			}
			setReadingRequest(true);
		}

		/**
		 * Treats the data prepared to be sent after being called by the
		 * selector
		 * 
		 * @throws IOException
		 */
		public void doWrite() throws IOException {
			out.flip();
			sc.write(out);
			out.compact();
			// ce process est mis en place pour traiter le cas où le buffer out
			// est vide donc on n'a plus rien à envoyer et au même temps le
			// buffer in est plein donc on ne peut plus lire
			process();
			updateInterestOps();
		}

		/**
		 * Updates the InterestOps
		 */
		private void updateInterestOps() {
			int ops = 0;
			if (out.position() != 0) {
				ops |= SelectionKey.OP_WRITE;
			}
			if (in.hasRemaining() && !inputClosed) {
				ops |= SelectionKey.OP_READ;
			}
			if (ops == 0) {
				silentlyClose(sc);
			} else {
				key.interestOps(ops);
			}
		}

		/**
		 * Set sendingPost to false and create a new HTTPReader for the next
		 * task
		 * 
		 * @param sc
		 *            the SocketChannel used by the HTTPReader
		 * @throws IOException
		 */
		public void reset(SocketChannel sc) throws IOException {
			setSendingPost(false);
			in.clear();
			reader = new HTTPServerReader(sc, in);
		}

		/**
		 * Reset the inactivity counter
		 */
		private void resetInactiveTime() {
			this.inactiveTime = 0L;
		}

		/**
		 * Adds time passed in processing to the inactivity time
		 * 
		 * @param time
		 *            to add
		 * @param timeout
		 *            to not to pass
		 */
		private void addInactiveTime(long time, long timeout) {
			this.inactiveTime += time;
			if (inactiveTime > timeout) {
				silentlyClose(sc);
			}
		}

		/**
		 * Parses a request string and dispatches the processing between GET
		 * method or POST method
		 * 
		 * @param sc
		 *            socket channel between the server and the client
		 * @throws IOException
		 */
		public void parseRequest(SocketChannel sc) throws IOException {
			String request = getRequest();
			String firstLine = request.split("\r\n")[0];
			String[] tokens = firstLine.split(" ");
			String command = tokens[0];
			String requestNature = tokens[1];
			String httpVersion = tokens[2];
			if (command.equals("GET") && requestNature.equals("Task") && httpVersion.equals("HTTP/1.1")) {
				if (!isParsingRequest()) {
					saveLog("Client " + sc.getRemoteAddress() + " is requesting a task");
				}
				setRequestingTask(true);
				setParsingRequest(true);
				if (isParsingRequest()) {
					while (!getReader().readLineCRLF().equals("")) {
						// Consumes all useless lines on the GET request
					}
					setParsingRequest(false);
				}
			} else if (command.equals("POST") && requestNature.equals("Answer") && httpVersion.equals("HTTP/1.1")) {
				if (!isParsingRequest()) {
					saveLog("Client " + sc.getRemoteAddress() + " is posting an answer");
				}
				setParsingRequest(true);
				try {
					String response = parsePostClientAnswer();
					updateResponse(response);
					setParsingRequest(false);
				} catch (IllegalStateException e) {
					// The client answer does not contain a json content
					out.put(CHARSET_UTF_8.encode(BAD_REQUEST));
					return;
				}
			} else {
				throw new IllegalStateException();
			}
		}

		/**
		 * Parses the POST response given by the client
		 * 
		 * @return String contains the response content
		 * @throws IOException
		 */
		private String parsePostClientAnswer() throws IOException {
			if (!isReadingAnswer()) {
				String line;
				while (!(line = reader.readLineCRLF()).equals("")) {
					String[] token = line.split(": ");
					if (token[0].equals("Content-Length")) {
						setContentLength(Integer.parseInt(token[1]));
					}
					if (token[0].equals("Content-Type")) {
						if (!token[1].equals("application/json")) {
							throw new IllegalStateException();
						}
					}
				}
				setReadingAnswer(true);
			}
			ByteBuffer content = reader.readBytes(getContentLength());
			setReadingAnswer(false);
			content.flip();
			long jobId = content.getLong();
			int taskNumber = content.getInt();
			String response = CHARSET_UTF_8.decode(content).toString();
			if (response != null && JsonUtils.isValidJsonString(response)) {
				ServerJarRet.saveAnswer(jobId, response);
			}
			return response;
		}

		public ByteBuffer getIn() {
			return in;
		}

		public ByteBuffer getOut() {
			return out;
		}

		public boolean isInputClosed() {
			return inputClosed;
		}

		public SelectionKey getKey() {
			return key;
		}

		public int getContentLength() {
			return contentLength;
		}

		public void setContentLength(int contentLength) {
			this.contentLength = contentLength;
		}

		public boolean isRequestingTask() {
			return requestingTask;
		}

		public void setRequestingTask(boolean requestingTask) {
			this.requestingTask = requestingTask;
		}

		public boolean isSendingPost() {
			return sendingPost;
		}

		public void setSendingPost(boolean sendingPost) {
			this.sendingPost = sendingPost;
		}

		public boolean isReadingRequest() {
			return readingRequest;
		}

		public void setReadingRequest(boolean readingRequest) {
			this.readingRequest = readingRequest;
		}

		public boolean isParsingRequest() {
			return parsingRequest;
		}

		public void setParsingRequest(boolean parsingRequest) {
			this.parsingRequest = parsingRequest;
		}

		public boolean isReadingAnswer() {
			return readingAnswer;
		}

		public void setReadingAnswer(boolean readingAnswer) {
			this.readingAnswer = readingAnswer;
		}

		public String getRequest() {
			return request;
		}

		public void setRequest(String request) {
			this.request = request;
		}

		public String getResponse() {
			return response;
		}

		public void setResponse(String response) {
			this.response = response;
		}

		public HTTPServerReader getReader() {
			return reader;
		}

		public void setReader(HTTPServerReader reader) {
			this.reader = reader;
		}

		public void updateResponse(String response) {
			this.setResponse(response);
			this.setSendingPost(true);
		}
	}

	private static final Charset CHARSET_UTF_8 = Charset.forName("UTF-8");
	private static final String HTTP_1_1_200_OK = "HTTP/1.1 200 OK\r\n";
	private static final String BAD_REQUEST = "HTTP/1.1 400 Bad Request\r\n\r\n";
	private static final int BUF_SIZE = 4096;
	private final ServerSocketChannel serverSocketChannel;
	private final Selector selector;
	private final Set<SelectionKey> selectedKeys;
	private static final long TIMEOUT = 3000;
	private Thread listener;
	private final static BlockingQueue<String> queue = new LinkedBlockingQueue<>(5);
	private static String responsesFolderPath;
	private static String logsFolderPath;
	private static long maxResponseFileSize;
	private static long comeBackInSeconds;
	private final static BlockingQueue<Job> jobs = new LinkedBlockingQueue<>();

	/**
	 * ServerJarRet Constructor
	 * 
	 * @param port
	 *            server port
	 * @throws IOException
	 */
	private ServerJarRet(int port) throws IOException {
		serverSocketChannel = ServerSocketChannel.open();
		serverSocketChannel.bind(new InetSocketAddress(port));
		selector = Selector.open();
		selectedKeys = selector.selectedKeys();
	}

	/**
	 * Parses the configuration file and gives the instance of ServerJarRet
	 * 
	 * @return instance of ServerJarRet
	 * @throws IOException
	 */
	public static ServerJarRet getServerJarRet() throws IOException {
		ObjectNode configs = JsonUtils.fromStringToJson(JsonUtils.parseJsonFile("JarRetConfig.json"));
		int serverPort = configs.get("ServerPort").asInt();
		ServerJarRet server = new ServerJarRet(serverPort);
		ServerJarRet.maxResponseFileSize = configs.get("MaxResponseFileSize").asLong();
		ServerJarRet.comeBackInSeconds = configs.get("ComeBackInSeconds").asLong();
		ServerJarRet.responsesFolderPath = configs.get("ResponsesFolderPath").asText();
		ServerJarRet.logsFolderPath = configs.get("LogsFolderPath").asText();
		System.out.println("ServerJarRet started and listening on port " + serverPort);
		return server;
	}

	/**
	 * Formats a job description from a Job POJO
	 * 
	 * @param job
	 *            job to convert into a job description
	 * @return String contains the job description
	 * @throws IOException
	 */
	public static String formatJobDescription(Job job) throws IOException {
		StringBuilder sb = new StringBuilder();
		sb.append("{\"JobId\": \"").append(job.getJobId()).append("\",").append("\"WorkerVersion\": \"")
				.append(job.getWorkerVersionNumber()).append("\", ").append("\"WorkerURL\": \"")
				.append(job.getWorkerURL()).append("\", ").append("\"WorkerClassName\": \"")
				.append(job.getWorkerClassName()).append("\", ").append("\"Task\": \"")
				.append(job.getIndexOfAvailableTask()).append("\"}");
		ObjectNode objectNode = JsonUtils.fromStringToJson(sb.toString());
		ObjectMapper mapper = new ObjectMapper();
		String responseContent = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(objectNode);
		return responseContent;
	}

	/**
	 * Formats an entire job description query to be sent to client
	 * 
	 * @param jobDescription
	 *            string contains the job description
	 * @return String contains the entire job description query
	 */
	public static String formatJobDescriptionAnswer(String jobDescription) {
		StringBuilder sb = new StringBuilder();
		sb.append(HTTP_1_1_200_OK).append("Content-Type: application/json; charset=utf-8\r\n")
				.append("Content-Length: ").append(CHARSET_UTF_8.encode(jobDescription).remaining()).append("\r\n")
				.append("\r\n").append(jobDescription);
		return sb.toString();
	}

	/**
	 * Sends the available task to the client
	 * 
	 * @param sc
	 *            socket channel between the server and the client
	 * @throws IOException
	 */
	private static ByteBuffer getAvailableTask(SocketChannel sc) throws IOException {
		ByteBuffer serverResponseContent;
		while (!jobs.isEmpty()) {
			Job job = jobs.poll();
			if (!job.isDone()) {
				try {
					String jobAsJson = formatJobDescription(job);
					serverResponseContent = CHARSET_UTF_8.encode(formatJobDescriptionAnswer(jobAsJson));
					return serverResponseContent;
				} catch (JsonProcessingException e) {
					saveLog("JsonProcessingException on writing back task to client");
					e.printStackTrace();
				}
				if (!job.isDone()) {
					jobs.add(job);
				}
			}
		}
		serverResponseContent = CHARSET_UTF_8.encode("\"ComeBackInSeconds\": " + comeBackInSeconds);
		return serverResponseContent;
	}

	/**
	 * Parses a Json file and return a List of ObjectNode from the file to load
	 * jobs to be sent to client
	 * 
	 * @param fileName
	 *            String file pathname
	 * @return List of {@link ObjectNode} the list of job descriptions to be
	 *         sent to clients
	 */
	private List<ObjectNode> loadJobDescriptions(String fileName) {
		ObjectMapper mapper = new ObjectMapper();
		List<ObjectNode> nodes = new LinkedList<ObjectNode>();
		try {
			JsonParser jsonParser = new JsonFactory().createParser(new File(fileName));
			MappingIterator<ObjectNode> jsonObject = mapper.readValues(jsonParser, ObjectNode.class);
			while (jsonObject.hasNext()) {
				ObjectNode node = jsonObject.next();
				int jobPriority = node.get("JobPriority").asInt();
				if (jobPriority != 0) {
					for (int i = 0; i < jobPriority; i++) {
						nodes.add(node);
					}
				}
			}
		} catch (JsonGenerationException e) {
			saveLog(e.getMessage());
			e.printStackTrace();
		} catch (JsonMappingException e) {
			saveLog(e.getMessage());
			e.printStackTrace();
		} catch (IOException e) {
			saveLog(e.getMessage());
			e.printStackTrace();
		}
		return nodes;
	}

	/**
	 * Converts a list of nodes to a list of jobs
	 * 
	 * @param nodes
	 *            list of nodes
	 * @return list of jobs
	 */
	private Optional<List<Job>> getJobsFromNodes(List<ObjectNode> nodes) {
		List<Job> jobs = new ArrayList<>();
		ObjectMapper mapper = new ObjectMapper();
		for (ObjectNode node : nodes) {
			try {
				jobs.add(mapper.treeToValue(node, Job.class));
			} catch (JsonProcessingException e) {
				saveLog(e.getMessage());
				return Optional.empty();
			}
		}
		return Optional.of(jobs);
	}

	/**
	 * Starts a thread to listen the commands entered by user
	 * 
	 * @param in
	 *            input stream that will be used to enter commands
	 */
	public void startCommandListener(InputStream in) {
		listener = new Thread(() -> {
			Scanner scanner = new Scanner(in);
			while (scanner.hasNextLine()) {
				try {
					queue.put(scanner.nextLine());
					selector.wakeup();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			scanner.close();
		});
		listener.setDaemon(true);
		listener.start();
	}

	/**
	 * Saves client responses into local files
	 * 
	 * @param jobId
	 *            long contains jobId
	 * @param response
	 *            String contains the response content
	 * @throws IOException
	 */
	private static void saveAnswer(long jobId, String response) throws IOException {
		int fileNumber = 1;
		long size = 0;
		Path responseFilePath;
		response += '\n';
		do {
			responseFilePath = Paths.get(responsesFolderPath + jobId + "_" + fileNumber++);
			if (Files.exists(responseFilePath, LinkOption.NOFOLLOW_LINKS)) {
				size = Files.size(responseFilePath);
			} else {
				break;
			}
		} while (size > maxResponseFileSize);

		try (BufferedWriter writer = Files.newBufferedWriter(responseFilePath, StandardOpenOption.APPEND,
				StandardOpenOption.CREATE); PrintWriter out = new PrintWriter(writer)) {
			out.println(response);
		} catch (IOException e) {
			System.err.println(e);
		}
	}

	/**
	 * Saves logging messages into a local file
	 * 
	 * @param message
	 *            String contains the message to save
	 */
	private static void saveLog(String message) {
		Path logFilePath = Paths.get(logsFolderPath + "log");
		try (BufferedWriter writer = Files.newBufferedWriter(logFilePath, StandardOpenOption.APPEND,
				StandardOpenOption.CREATE); PrintWriter outLog = new PrintWriter(writer)) {
			outLog.println(message);
		} catch (IOException e) {
			System.err.println(e);
		}
	}

	/**
	 * Launches the ServerJarRet
	 * 
	 * @throws IOException
	 */
	public void launch() throws IOException {
		List<ObjectNode> jobNodes = loadJobDescriptions("JobsDescriptions.json");
		Optional<List<Job>> jobsOptional = getJobsFromNodes(jobNodes);
		if (jobsOptional.isPresent()) {
			jobs.addAll(jobsOptional.get());
			serverSocketChannel.configureBlocking(false);
			serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
			Set<SelectionKey> selectedKeys = selector.selectedKeys();
			while (!Thread.interrupted()) {
				long startLoop = System.currentTimeMillis();
				selector.select(TIMEOUT / 10);
				processCommands();
				processSelectedKeys();
				long endLoop = System.currentTimeMillis();
				long timeSpent = endLoop - startLoop;
				updateInactivityKeys(timeSpent);
				selectedKeys.clear();
			}
			selector.close();
		} else {
			saveLog("Error during json file parsing [loading jobs] !");
			System.err.println("Error during json file parsing [loading jobs] !");
		}
	}

	/**
	 * Treats the different user commands to interact with server
	 * 
	 * @throws IOException
	 */
	private void processCommands() throws IOException {
		String command;
		while ((command = queue.poll()) != null) {
			switch (command.toUpperCase()) {
			case "HALT":
				System.out.println("HALTING SERVER");
				shutdownNow();
				break;
			case "STOP":
				System.out.println("STOPPING SERVER");
				shutdown();
				break;
			case "FLUSH":
				System.out.println("FLUSHING SERVER");
				haltClients();
				break;
			case "SHOW":
				System.out.println("SHOWING SERVER INFORMATION");
				System.out.println("Clients count : " + (selector.keys().size() - 1));
				break;
			default:
				System.out.println("Unknown command");
				break;
			}
		}
	}

	/**
	 * Updates the inactivity times for all the keys in the selector
	 * 
	 * @param timeSpent
	 *            long contains the amount to add to the inactivity time
	 */
	private void updateInactivityKeys(long timeSpent) {
		for (SelectionKey key : selector.keys()) {
			if (key.attachment() != null) {
				Context context = (Context) key.attachment();
				context.addInactiveTime(timeSpent, TIMEOUT);
			}
		}
	}

	/**
	 * Halts all clients
	 */
	private void haltClients() {
		for (SelectionKey key : selector.keys()) {
			if (key.attachment() != null) {
				Context context = (Context) key.attachment();
				silentlyClose(context.sc);
				key.cancel();
			}
		}
	}

	/**
	 * Halts the server after finishing process
	 * 
	 * @throws IOException
	 */
	private void shutdown() throws IOException {
		silentlyClose(serverSocketChannel);
	}

	/**
	 * Halts the server immediately
	 * 
	 * @throws IOException
	 */
	private void shutdownNow() throws IOException {
		silentlyClose(serverSocketChannel);
		haltClients();
		Thread.currentThread().interrupt();
	}

	/**
	 * Treats all the selected keys by the selector
	 * 
	 * @throws IOException
	 */
	private void processSelectedKeys() throws IOException {
		for (SelectionKey key : selectedKeys) {
			if (key.isValid() && key.isAcceptable()) {
				doAccept(key);
				continue;
			}
			try {
				Context cntxt = (Context) key.attachment();
				if (key.isValid() && key.isWritable()) {
					cntxt.doWrite();
				}
				if (key.isValid() && key.isReadable()) {
					cntxt.doRead();
				}
			} catch (IOException e) {
				silentlyClose(key.channel());
			}
		}
	}

	/**
	 * Accepts new clients
	 * 
	 * @param key
	 *            SelectionKey given by the selector
	 * @throws IOException
	 */
	private void doAccept(SelectionKey key) throws IOException {
		SocketChannel sc = serverSocketChannel.accept();
		sc.configureBlocking(false);
		SelectionKey clientKey = sc.register(selector, SelectionKey.OP_READ);
		clientKey.attach(new Context(clientKey));
	}

	/**
	 * Silently closes a socket channel
	 * 
	 * @param sc
	 *            socket channel to be closed
	 */
	private static void silentlyClose(SelectableChannel sc) {
		if (sc == null)
			return;
		try {
			sc.close();
		} catch (IOException e) {
			// silently ignore
		}
	}

	/**
	 * Informs user about how to use this class
	 */
	private static void usage() {
		System.out.println("ServerJarRet");
	}

	public static void main(String[] args) throws NumberFormatException, IOException {
		if (args.length > 0) {
			usage();
			return;
		}
		ServerJarRet server = ServerJarRet.getServerJarRet();
		server.startCommandListener(System.in);
		server.launch();
	}
}