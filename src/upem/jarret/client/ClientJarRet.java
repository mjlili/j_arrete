package upem.jarret.client;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Objects;
import java.util.Optional;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import upem.jarret.worker.Worker;
import upem.jarret.worker.WorkerFactory;
import utils.JsonUtils;

public class ClientJarRet {

	private static final Charset CHARSET_UTF_8 = Charset.forName("UTF-8");
	private static final int MAX_BUFFER_SIZE = 4096;

	private String serverAddress;
	private static SocketAddress server;
	private String clientId;
	private static SocketChannel socketChannel;
	private HTTPHeader currentHeader;
	private String jobDescription;
	private final HashMap<Long, HashMap<String, Worker>> workers;

	/**
	 * ClientJarRet constructor
	 * 
	 * @param clientId
	 *            the client id
	 * @param serverAddress
	 *            the server address
	 * @param port
	 *            the server port
	 */
	public ClientJarRet(String clientId, String serverAddress, int port) {
		this.clientId = Objects.requireNonNull(clientId);
		this.serverAddress = serverAddress;
		ClientJarRet.server = new InetSocketAddress(Objects.requireNonNull(serverAddress), port);
		this.workers = new LinkedHashMap<>();
	}

	/**
	 * Searches for an existing worker for the jobId and the workerVersion given
	 * in the objectNode
	 * 
	 * @param objecNode
	 *            contains information about the requested worker
	 * @return an Optional<Worker> which contains the requested worker or empty
	 *         if its instance was not found
	 */
	private Optional<Worker> getExistingWorkerInstance(ObjectNode objecNode) {
		HashMap<String, Worker> workerByJobId = this.workers.get(objecNode.get("JobId"));
		if (workerByJobId != null) {
			Worker workerByVersion = workerByJobId.get(objecNode.get("WorkerVersion"));
			if (workerByVersion != null) {
				return Optional.of(workerByVersion);
			}
		}
		return Optional.empty();
	}

	/**
	 * Instantiates a new instance of Worker if it was not found on the workers
	 * list or gives the existing one
	 * 
	 * @param objectNode
	 *            contains information about the requested worker
	 * @return an existing instance of worker or a new one
	 */
	private Worker getFinalWorkerInstance(ObjectNode objectNode) {
		Worker worker = null;
		Optional<Worker> existingWorker = getExistingWorkerInstance(objectNode);
		if (existingWorker.isPresent()) {
			worker = existingWorker.get();
		} else {
			try {
				String workerUrl = objectNode.get("WorkerURL").asText();
				String workerClassName = objectNode.get("WorkerClassName").asText();
				worker = WorkerFactory.getWorker(workerUrl, workerClassName);
				HashMap<String, Worker> workerByVersion = new LinkedHashMap<>();
				workerByVersion.put(objectNode.get("WorkerVersion").asText(), worker);
				workers.put(objectNode.get("JobId").asLong(), workerByVersion);
			} catch (MalformedURLException | ClassNotFoundException | IllegalAccessException
					| InstantiationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return worker;
	}

	/**
	 * Does the computation for a given task number using an instance of a
	 * worker
	 * 
	 * @param objectNode
	 *            contains information about the requested worker
	 * @return String containing the result of the computation
	 * @throws IOException
	 *             in case of a computation error
	 */
	private Optional<String> launchComputation(ObjectNode objectNode) throws IOException {
		System.out.println("Retrieving worker");
		Worker worker = getFinalWorkerInstance(objectNode);
		if (worker == null) {
			System.out.println("Error retieving worker class");
			sendComputationErrorResponse();
			return Optional.empty();
		}
		int taskId = objectNode.get("Task").asInt();
		System.out.println("Starting computation");
		String result = worker.compute(taskId);
		if (result == null) {
			// SEND "Computation error" to SERVER
			sendComputationErrorResponse();
			return Optional.empty();
		}
		if (!JsonUtils.isValidJsonString(result)) {
			sendNotJsonErrorResponse();
			return Optional.empty();
		}
		return Optional.of(result);
	}

	/**
	 * Prepares a GET request to the server
	 * 
	 * @throws IOException
	 *             in case of a writing problem in the socket channel
	 */
	private void sendGetTaskRequest() throws IOException {
		// this.socketChannel = SocketChannel.open();
		// this.socketChannel.connect(server);
		System.out.println("Asking for a new Task	");
		String request = "GET Task HTTP/1.1\r\n" + "Host: " + serverAddress + "\r\n" + "\r\n";
		ClientJarRet.socketChannel.write(CHARSET_UTF_8.encode(request));
	}

	/**
	 * Prepares a computation error response
	 * 
	 * @throws IOException
	 *             in case of a sending problem in the socket channel
	 */
	private void sendComputationErrorResponse() throws IOException {
		ObjectNode objectNode = JsonUtils.fromStringToJson(jobDescription);
		objectNode.put("ClientId", clientId);
		objectNode.put("Error", "Computation error");
		sendErrorResponse(objectNode);
	}

	/**
	 * Prepares a too long error response in case of a computation result string
	 * that is too long to be accepted by the server (4096 bytes)
	 * 
	 * @throws IOException
	 *             in case of a sending problem in the socket channel
	 */
	private void sendTooLongErrorResponse() throws IOException {
		ObjectNode objectNode = JsonUtils.fromStringToJson(jobDescription);
		objectNode.put("ClientId", clientId);
		objectNode.put("Error", "Too Long");
		sendErrorResponse(objectNode);
	}

	/**
	 * Prepares a nested error response in case of a computation result string
	 * that contains an OBJECT field
	 * 
	 * @throws IOException
	 *             in case of a sending problem in the socket channel
	 */
	private void sendAnswerNestedErrorResponse() throws IOException {
		ObjectNode objectNode = JsonUtils.fromStringToJson(jobDescription);
		objectNode.put("ClientId", clientId);
		objectNode.put("Error", "Answer is nested");
		sendErrorResponse(objectNode);
	}

	/**
	 * Prepares a not json error in case of a string that is not representing a
	 * json object
	 * 
	 * @throws IOException
	 *             in case of a sending problem in the socket channel
	 */
	private void sendNotJsonErrorResponse() throws IOException {
		ObjectNode objectNode = JsonUtils.fromStringToJson(jobDescription);
		objectNode.put("ClientId", clientId);
		objectNode.put("Error", "Answer is not valid JSON");
		sendErrorResponse(objectNode);
	}

	/**
	 * Sends any kind of error response to the server
	 * 
	 * @param objectNode
	 *            contains information about the error response
	 * @throws IOException
	 *             in case of a writing problem in the socket channel
	 */
	private void sendErrorResponse(ObjectNode objectNode) throws IOException {
		String requestHeader = "POST Answer " + currentHeader.getVersion() + "\r\n" + "Host: " + serverAddress + "\r\n"
				+ "Content-Type: " + currentHeader.getContentType() + "\r\n" + "Content-Length: "
				+ objectNode.toString().length() + "\r\n" + "\r\n";
		ByteBuffer bufferToSend = ByteBuffer.allocate(MAX_BUFFER_SIZE);
		bufferToSend.put(CHARSET_UTF_8.encode(requestHeader));
		bufferToSend.putLong(objectNode.get("JobId").asLong());
		bufferToSend.putInt(objectNode.get("Task").asInt());
		ClientJarRet.socketChannel.write(CHARSET_UTF_8.encode(requestHeader + objectNode.toString()));
	}

	/**
	 * Connects the client's socket channel to the server
	 */
	public static void connect() {
		try {
			socketChannel.close();
		} catch (Exception e) {
			//
		}
		while (true) {
			System.out.println("Trying to connect with server...");
			try {
				socketChannel = SocketChannel.open();
				socketChannel.connect(server);
				return;
			} catch (ConnectException e) {
				//
			} catch (IOException e) {
				//
			}
			try {
				Thread.sleep(300);
			} catch (IllegalArgumentException e) {
				return;
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * Receives a job description from the server
	 * 
	 * @return Optional<String> containing the job description string and empty
	 *         if something wrong happens
	 * @throws IOException
	 *             in case of problems in connection with server
	 */
	private Optional<String> receiveTaskFromServer() throws IOException {
		ByteBuffer buffer = ByteBuffer.allocate(MAX_BUFFER_SIZE);
		HTTPReader reader = new HTTPReader(socketChannel, buffer);
		currentHeader = reader.readHeader();
		int code = currentHeader.getCode();
		if (code == 301) {
			throw new HTTPException("The asked resource was definitively moved");
		} else if (code == 302) {
			throw new HTTPException("The asked resource was found but with another address");
		} else if (code == 400) {
			System.out.println("BAD REQUEST");
			return Optional.empty();
		}
		int contentLength = currentHeader.getContentLength();
		ByteBuffer contentBuffer = reader.readBytes(contentLength);
		contentBuffer.flip();
		jobDescription = CHARSET_UTF_8.decode(contentBuffer).toString();
		System.out.println(jobDescription);
		return Optional.of(jobDescription);
	}

	/**
	 * Sends the final response to the server
	 * 
	 * @param objectNode
	 *            contains information about the response
	 * @param computationResult
	 *            contains the computation's result string
	 * @throws JsonProcessingException
	 *             in case of problems in writing prettily the content
	 */
	public void sendBackAnswer(ObjectNode objectNode, ObjectNode computationResult) throws JsonProcessingException {
		objectNode.put("ClientId", clientId);
		objectNode.set("Answer", computationResult);
		ObjectMapper mapper = new ObjectMapper();
		String answerContent = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(objectNode);
		StringBuilder answerHeaderBuilder = new StringBuilder();
		answerHeaderBuilder.append("POST Answer ").append(currentHeader.getVersion()).append("\r\n").append("Host: ")
				.append(serverAddress).append("\r\n").append("Content-Type: ").append(currentHeader.getContentType())
				.append("\r\n").append("Content-Length: ")
				.append(CHARSET_UTF_8.encode(answerContent).remaining() + Integer.BYTES + Long.BYTES).append("\r\n")
				.append("\r\n");
		// TOOLONG RESPONSE
		if (CHARSET_UTF_8.encode(answerHeaderBuilder.toString()).remaining()
				+ CHARSET_UTF_8.encode(answerContent).remaining() + Integer.BYTES + Long.BYTES > MAX_BUFFER_SIZE) {
			try {
				sendTooLongErrorResponse();
			} catch (IOException e) {
				System.out.println("Reconnexion ...");
				connect();
			}
			return;
		}
		System.out.println("Writing answer to server");
		ByteBuffer bufferToSend = ByteBuffer.allocate(MAX_BUFFER_SIZE);
		bufferToSend.put(CHARSET_UTF_8.encode(answerHeaderBuilder.toString()));
		bufferToSend.putLong(objectNode.get("JobId").asLong());
		bufferToSend.putInt(objectNode.get("Task").asInt());
		bufferToSend.put(CHARSET_UTF_8.encode(answerContent));
		bufferToSend.flip();
		// BEGIN TESTING
		ByteBuffer bufferToTest = ByteBuffer.allocate(MAX_BUFFER_SIZE);
		bufferToTest.put(CHARSET_UTF_8.encode(answerHeaderBuilder.toString()));
		bufferToTest.putLong(objectNode.get("JobId").asLong());
		bufferToTest.putInt(objectNode.get("Task").asInt());
		bufferToTest.put(CHARSET_UTF_8.encode(answerContent));
		bufferToTest.flip();
		System.err.println(CHARSET_UTF_8.decode(bufferToTest).toString());
		// END TESTING
		try {
			System.out.println("SENT : " + ClientJarRet.socketChannel.write(bufferToSend));
		} catch (IOException e) {
			System.out.println("Reconnexion ...");
			connect();
		}
		silentlyClose(ClientJarRet.socketChannel);
	}

	/**
	 * Silently closes a socket channel
	 * 
	 * @param sc
	 *            the sochket channel to be closed
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

	public static void main(String[] args) throws IOException, NumberFormatException, InterruptedException {
		if (args.length < 3) {
			usage();
			return;
		}
		ClientJarRet client = new ClientJarRet(args[0], args[1], Integer.parseInt(args[2]));
		while (!Thread.interrupted()) {
			ClientJarRet.connect();
			client.sendGetTaskRequest();
			Optional<String> task = client.receiveTaskFromServer();
			if (task.isPresent()) {
				ObjectNode objectNode = JsonUtils.fromStringToJson(task.get());
				if (objectNode.get("ComeBackInSeconds") != null) {
					System.out.println("SLEEPING for : "
							+ Integer.parseInt(objectNode.get("ComeBackInSeconds").asText()) + " seconds");
					Thread.sleep(Integer.parseInt(objectNode.get("ComeBackInSeconds").asText()) * 1000);
					continue;
				}
				System.out.println("Received Task " + objectNode.get("Task") + " for " + objectNode.get("JobId") + " ("
						+ objectNode.get("WorkerURL") + ", " + objectNode.get("WorkerClassName") + ", "
						+ objectNode.get("WorkerVersion") + ")");
				Optional<String> computationResultOptional = client.launchComputation(objectNode);
				if (computationResultOptional.isPresent()) {
					ObjectNode computationResult = JsonUtils.fromStringToJson(computationResultOptional.get());
					if (JsonUtils.jsonContainsObjectField(computationResult)) {
						client.sendAnswerNestedErrorResponse();
						continue;
					}
					client.sendBackAnswer(objectNode, computationResult);
				}
			}
		}
	}

	/**
	 * Informs user about how to use this class
	 */
	private static void usage() {
		System.out.println("Usage : ClientJarRet clientID serverAddress port");
	}
}
