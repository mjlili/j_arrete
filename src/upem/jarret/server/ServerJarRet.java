package upem.jarret.server;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class ServerJarRet {

	private static class Context {
		private boolean inputClosed = false;
		private final ByteBuffer in = ByteBuffer.allocate(BUF_SIZE);
		private final ByteBuffer out = ByteBuffer.allocate(BUF_SIZE);
		private final SelectionKey key;
		private final SocketChannel sc;
		private long inactiveTime;

		public Context(SelectionKey key) {
			this.key = key;
			this.sc = (SocketChannel) key.channel();
		}

		public void doRead() throws IOException {
			if (sc.read(in) == -1) {
				inputClosed = true;
			}
			resetInactiveTime();
			process();
			updateInterestOps();
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

		private void process() {
			in.flip();
			if (in.remaining() < 2 * Integer.BYTES) {
				in.compact();
				return;
			}
			int opsCount = in.remaining() / (2 * Integer.BYTES);
			for (int i = 0; i < opsCount; i++) {
				out.putInt(in.getInt() + in.getInt());
			}
			in.compact();
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

	}

	private static final int BUF_SIZE = 512;
	private final ServerSocketChannel serverSocketChannel;
	private final Selector selector;
	private final Set<SelectionKey> selectedKeys;
	private static final long TIMEOUT = 3000;
	private Thread listener;
	private final static BlockingQueue<String> queue = new LinkedBlockingQueue<>(5);
	private String responsesFolderPath;
	private String logsFolderPath;
	private int serverPort;
	private long maxResponseFileSize;
	private long comeBackInSeconds;

	private ServerJarRet(int port) throws IOException {
		serverSocketChannel = ServerSocketChannel.open();
		serverSocketChannel.bind(new InetSocketAddress(port));
		selector = Selector.open();
		selectedKeys = selector.selectedKeys();
	}

	public static ServerJarRet getServerJarRet() throws IOException {
		ObjectNode configs = fromStringToJson(parseJsonFile("JarRetConfig.json"));
		int serverPort = configs.get("ServerPort").asInt();
		ServerJarRet server = new ServerJarRet(serverPort);
		server.maxResponseFileSize = configs.get("MaxResponseFileSize").asLong();
		server.comeBackInSeconds = configs.get("ComeBackInSeconds").asLong();
		server.responsesFolderPath = configs.get("ResponsesFolderPath").asText();
		server.logsFolderPath = configs.get("LogsFolderPath").asText();
		return server;
	}

	private static String parseJsonFile(String path) throws IOException {
		Path filePath = Paths.get(path);
		BufferedReader reader = Files.newBufferedReader(filePath);
		StringBuilder sb = new StringBuilder();
		reader.lines().forEach(line -> sb.append(line));
		reader.close();
		return sb.toString();
	}

	private static ObjectNode fromStringToJson(String content) throws JsonProcessingException, IOException {
		ObjectMapper mapper = new ObjectMapper();
		JsonNode node = mapper.readTree(content);
		ObjectNode objectNode = (ObjectNode) node;
		return objectNode;
	}

	public void startCommandListener(InputStream in) {
		listener = new Thread(() -> {
			Scanner scanner = new Scanner(in);
			while (scanner.hasNext()) {
				try {
					queue.put(scanner.next());
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

	public void launch() throws IOException {
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
		System.out.println("ServerSumNew <listeningPort>");
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
		if (args.length != 1) {
			usage();
			return;
		}
		ServerJarRet server = ServerJarRet.getServerJarRet();
		server.startCommandListener(System.in);
		server.launch();
	}
}