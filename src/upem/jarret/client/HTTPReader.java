package upem.jarret.client;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.LinkedHashMap;

public class HTTPReader {
	private final SocketChannel sc;
	private final ByteBuffer buff;

	public HTTPReader(SocketChannel sc, ByteBuffer buff) {
		this.sc = sc;
		this.buff = buff;
	}

	/**
	 * @return The ASCII string terminated by CRLF
	 *         <p>
	 *         The method assume that buff is in write mode and leave it in
	 *         write-mode The method never reads from the socket as long as the
	 *         buffer is not empty
	 * @throws IOException
	 *             HTTPException if the connection is closed before a line could
	 *             be read
	 */
	public String readLineCRLF() throws IOException {
		StringBuilder sb = new StringBuilder();
		boolean lastCR = false;
		while (true) {
			buff.flip();
			while (buff.hasRemaining()) {
				char currentChar = (char) buff.get();
				sb.append(currentChar);
				if (currentChar == '\n' && lastCR) {
					sb.setLength(sb.length() - 2);
					buff.compact();
					return sb.toString();
				}
				lastCR = (currentChar == '\r');
			}
			buff.compact();
			if (sc.read(buff) == -1) {
				// IL FAUT TENTER DE SE RECONNECTER AU SERVEUR
				ClientJarRet.connect();
				// throw new HTTPException("Connection with server is closed");
			}
		}
	}

	/**
	 * @return The HTTPHeader object corresponding to the header read
	 * @throws IOException
	 *             HTTPException if the connection is closed before a header
	 *             could be read if the header is ill-formed
	 */
	public HTTPHeader readHeader() throws IOException {
		String response = readLineCRLF();
		HashMap<String, String> fields = new LinkedHashMap<>();
		String line;
		while (!(line = readLineCRLF()).equals("")) {
			String key = line.substring(0, line.indexOf(':'));
			String value = line.substring(line.indexOf(':') + 2);
			String oldValue;
			if ((oldValue = fields.get(key)) != null) {
				value = new StringBuilder(oldValue).append("; ").append(value).toString();
			}
			fields.put(key, value);
		}
		if (!line.equals("")) {
			// IL FAUT TENTER DE SE RECONNECTER AU SERVEUR
			// ClientJarRet.connect();
			throw new HTTPException("Connection with server is closed");
		}
		return HTTPHeader.create(response, fields);
	}

	static boolean readFully(ByteBuffer buffer, SocketChannel socketChannel) throws IOException {
		while (buffer.hasRemaining()) {
			if (socketChannel.read(buffer) == -1) {
				return false;
			}
		}
		return true;
	}

	/**
	 * @param size
	 * @return a ByteBuffer in write-mode containing size bytes read on the
	 *         socket
	 * @throws IOException
	 *             HTTPException is the connection is closed before all bytes
	 *             could be read
	 */
	public ByteBuffer readBytes(int size) throws IOException {
		ByteBuffer buffer = ByteBuffer.allocate(size);
		buff.flip();
		buffer.put(buff);
		buff.compact();
		if (!readFully(buffer, sc)) {
			// IL FAUT TENTER DE SE RECONNECTER AU SERVEUR
			// ClientJarRet.connect();
			throw new HTTPException("Connection with server is closed");
		}
		return buffer;
	}
}
