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

//Il faut tenter de se reconnecter en cas de fermeture de la connexion du côté serveur
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

	public ClientJarRet(String clientId, String serverAddress, int port) throws IOException {
		this.clientId = Objects.requireNonNull(clientId);
		this.serverAddress = serverAddress;
		ClientJarRet.server = new InetSocketAddress(Objects.requireNonNull(serverAddress), port);
		this.workers = new LinkedHashMap<>();
	}

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

	private void sendGetTaskRequest() throws IOException {
		// this.socketChannel = SocketChannel.open();
		// this.socketChannel.connect(server);
		System.out.println("Asking for a new Task	");
		String request = "GET Task HTTP/1.1\r\n" + "Host: " + serverAddress + "\r\n" + "\r\n";
		ClientJarRet.socketChannel.write(CHARSET_UTF_8.encode(request));
	}

	private void sendComputationErrorResponse() throws IOException {
		ObjectNode objectNode = JsonUtils.fromStringToJson(jobDescription);
		objectNode.put("ClientId", clientId);
		objectNode.put("Error", "Computation error");
		sendErrorResponse(objectNode);
	}

	private void sendTooLongErrorResponse() throws IOException {
		ObjectNode objectNode = JsonUtils.fromStringToJson(jobDescription);
		objectNode.put("ClientId", clientId);
		objectNode.put("Error", "Too Long");
		sendErrorResponse(objectNode);
	}

	private void sendAnswerNestedErrorResponse() throws IOException {
		ObjectNode objectNode = JsonUtils.fromStringToJson(jobDescription);
		objectNode.put("ClientId", clientId);
		objectNode.put("Error", "Answer is nested");
		sendErrorResponse(objectNode);
	}

	private void sendNotJsonErrorResponse() throws IOException {
		ObjectNode objectNode = JsonUtils.fromStringToJson(jobDescription);
		objectNode.put("ClientId", clientId);
		objectNode.put("Error", "Answer is not valid JSON");
		sendErrorResponse(objectNode);
	}

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
		if (answerHeaderBuilder.toString().length() + objectNode.toString().length() > MAX_BUFFER_SIZE) {
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
		try {
			ClientJarRet.socketChannel.write(bufferToSend);
		} catch (IOException e) {
			System.out.println("Reconnexion ...");
			connect();
		}
		silentlyClose(ClientJarRet.socketChannel);
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

	private static void usage() {
		System.out.println("Usage : ClientJarRet clientID serverAddress port");
	}
}
