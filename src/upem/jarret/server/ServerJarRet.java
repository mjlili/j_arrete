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
import utils.JsonTools;

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

		public Context(SelectionKey key) throws IOException {
			this.key = key;
			this.sc = (SocketChannel) key.channel();
			this.reader = new HTTPServerReader(sc, in);
		}

		// public void clean(SocketChannel sc) throws IOException {
		// setSendingPost(false);
		// in.clear();
		// reader = new HTTPServerReader(sc, in);
		// }

		public void doRead() throws IOException {
			if (sc.read(in) == -1) {
				inputClosed = true;
			}
			resetInactiveTime();
			process();
			updateInterestOps();
		}

		private void process() throws IOException {
			// in.flip();
			if (isReadingRequest()) {
				try {
					setRequest(reader.readLineCRLF());
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
			}
			// else if (isSendingPost()) {
			// // sendCheckCode(key);
			// key.interestOps(SelectionKey.OP_READ);
			// }
			setReadingRequest(true);
		}

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

		private void resetInactiveTime() {
			this.inactiveTime = 0L;
		}

		private void addInactiveTime(long time, long timeout) {
			this.inactiveTime += time;
			if (inactiveTime > timeout) {
				silentlyClose(sc);
			}
		}

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
					String answer = parsePOST();
					requestResponse(answer);
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

		private String parsePOST() throws IOException {
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
			if (response != null && JsonTools.isJSON(response)) {
				ServerJarRet.saveAnswer(jobId, taskNumber, response);
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

		public void requestResponse(String response) {
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

	private ServerJarRet(int port) throws IOException {
		serverSocketChannel = ServerSocketChannel.open();
		serverSocketChannel.bind(new InetSocketAddress(port));
		selector = Selector.open();
		selectedKeys = selector.selectedKeys();
	}

	public static ServerJarRet getServerJarRet() throws IOException {
		ObjectNode configs = JsonTools.fromStringToJson(JsonTools.parseJsonFile("JarRetConfig.json"));
		int serverPort = configs.get("ServerPort").asInt();
		ServerJarRet server = new ServerJarRet(serverPort);
		ServerJarRet.maxResponseFileSize = configs.get("MaxResponseFileSize").asLong();
		ServerJarRet.comeBackInSeconds = configs.get("ComeBackInSeconds").asLong();
		ServerJarRet.responsesFolderPath = configs.get("ResponsesFolderPath").asText();
		ServerJarRet.logsFolderPath = configs.get("LogsFolderPath").asText();
		System.out.println("ServerJarRet started and listening on port " + serverPort);
		return server;
	}

	public static String formatJobDescription(Job job) throws IOException {
		StringBuilder sb = new StringBuilder();
		sb.append("{\"JobId\": \"").append(job.getJobId()).append("\",").append("\"WorkerVersion\": \"")
				.append(job.getWorkerVersionNumber()).append("\", ").append("\"WorkerURL\": \"")
				.append(job.getWorkerURL()).append("\", ").append("\"WorkerClassName\": \"")
				.append(job.getWorkerClassName()).append("\", ").append("\"Task\": \"")
				.append(job.getIndexOfAvailableTask()).append("\"}");
		ObjectNode objectNode = JsonTools.fromStringToJson(sb.toString());
		ObjectMapper mapper = new ObjectMapper();
		String responseContent = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(objectNode);
		System.out.println(responseContent);
		return responseContent;
	}

	public static String formatJobDescriptionAnswer(String jobDescription) {
		StringBuilder sb = new StringBuilder();
		sb.append(HTTP_1_1_200_OK).append("Content-Type: application/json; charset=utf-8\r\n")
				.append("Content-Length: ").append(CHARSET_UTF_8.encode(jobDescription).remaining()).append("\r\n")
				.append("\r\n").append(jobDescription);
		return sb.toString();
	}

	/**
	 * Sends the task to the client
	 * 
	 * @param sc
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
	 * Parses a Json file and return a List of ObjectNode from the file
	 * 
	 * @param file
	 *            String file pathname
	 * @return List of {@link ObjectNode} the list of json documents to be
	 *         inserted in the database
	 */
	private List<ObjectNode> parseJsonJobsDescriptionFile(String file) {
		ObjectMapper mapper = new ObjectMapper();
		List<ObjectNode> nodes = new LinkedList<ObjectNode>();
		try {
			JsonParser jsonParser = new JsonFactory().createParser(new File(file));
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

	private static void saveAnswer(long jobId, int taskNum, String response) throws IOException {
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

	private static void saveLog(String log) {
		Path logFilePath = Paths.get(logsFolderPath + "log");
		try (BufferedWriter writer = Files.newBufferedWriter(logFilePath, StandardOpenOption.APPEND,
				StandardOpenOption.CREATE); PrintWriter outLog = new PrintWriter(writer)) {
			outLog.println(log);
		} catch (IOException e) {
			System.err.println(e);
		}
	}

	public void launch() throws IOException {
		List<ObjectNode> jobNodes = parseJsonJobsDescriptionFile("JobsDescriptions.json");
		Optional<List<Job>> jobsOptional = getJobsFromNodes(jobNodes);
		if (jobsOptional.isPresent()) {
			jobs.addAll(jobsOptional.get());
			serverSocketChannel.configureBlocking(false);
			serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
			Set<SelectionKey> selectedKeys = selector.selectedKeys();
			while (!Thread.interrupted()) {
				// printKeys();
				// System.out.println("Starting select");
				long startLoop = System.currentTimeMillis();
				selector.select(TIMEOUT / 10);
				processCommands();
				// printSelectedKey();
				// System.out.println("Select finished");
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

	private void updateInactivityKeys(long timeSpent) {
		for (SelectionKey key : selector.keys()) {
			if (key.attachment() != null) {
				Context context = (Context) key.attachment();
				context.addInactiveTime(timeSpent, TIMEOUT);
			}
		}
	}

	private void haltClients() {
		for (SelectionKey key : selector.keys()) {
			if (key.attachment() != null) {
				Context context = (Context) key.attachment();
				silentlyClose(context.sc);
				key.cancel();
			}
		}
	}

	private void shutdown() throws IOException {
		silentlyClose(serverSocketChannel);
	}

	private void shutdownNow() throws IOException {
		silentlyClose(serverSocketChannel);
		haltClients();
		Thread.currentThread().interrupt();
	}

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

	private void doAccept(SelectionKey key) throws IOException {
		SocketChannel sc = serverSocketChannel.accept();
		sc.configureBlocking(false);
		SelectionKey clientKey = sc.register(selector, SelectionKey.OP_READ);
		clientKey.attach(new Context(clientKey));
	}

	private static void silentlyClose(SelectableChannel sc) {
		if (sc == null)
			return;
		try {
			sc.close();
		} catch (IOException e) {
			// silently ignore
		}
	}

	private static void usage() {
		System.out.println("ServerJarRet");
	}

	/***
	 * Theses methods are here to help understanding the behavior of the
	 * selector
	 ***/

	private String interestOpsToString(SelectionKey key) {
		if (!key.isValid()) {
			return "CANCELLED";
		}
		int interestOps = key.interestOps();
		ArrayList<String> list = new ArrayList<>();
		if ((interestOps & SelectionKey.OP_ACCEPT) != 0)
			list.add("OP_ACCEPT");
		if ((interestOps & SelectionKey.OP_READ) != 0)
			list.add("OP_READ");
		if ((interestOps & SelectionKey.OP_WRITE) != 0)
			list.add("OP_WRITE");
		return String.join("|", list);
	}

	public void printKeys() {
		Set<SelectionKey> selectionKeySet = selector.keys();
		if (selectionKeySet.isEmpty()) {
			System.out.println("The selector contains no key : this should not happen!");
			return;
		}
		System.out.println("The selector contains:");
		for (SelectionKey key : selectionKeySet) {
			SelectableChannel channel = key.channel();
			if (channel instanceof ServerSocketChannel) {
				System.out.println("\tKey for ServerSocketChannel : " + interestOpsToString(key));
			} else {
				SocketChannel sc = (SocketChannel) channel;
				System.out.println("\tKey for Client " + remoteAddressToString(sc) + " : " + interestOpsToString(key));
			}
		}
	}

	private String remoteAddressToString(SocketChannel sc) {
		try {
			return sc.getRemoteAddress().toString();
		} catch (IOException e) {
			return "???";
		}
	}

	private void printSelectedKey() {
		if (selectedKeys.isEmpty()) {
			System.out.println("There were not selected keys.");
			return;
		}
		System.out.println("The selected keys are :");
		for (SelectionKey key : selectedKeys) {
			SelectableChannel channel = key.channel();
			if (channel instanceof ServerSocketChannel) {
				System.out.println("\tServerSocketChannel can perform : " + possibleActionsToString(key));
			} else {
				SocketChannel sc = (SocketChannel) channel;
				System.out.println(
						"\tClient " + remoteAddressToString(sc) + " can perform : " + possibleActionsToString(key));
			}

		}
	}

	private String possibleActionsToString(SelectionKey key) {
		if (!key.isValid()) {
			return "CANCELLED";
		}
		ArrayList<String> list = new ArrayList<>();
		if (key.isAcceptable())
			list.add("ACCEPT");
		if (key.isReadable())
			list.add("READ");
		if (key.isWritable())
			list.add("WRITE");
		return String.join(" and ", list);
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