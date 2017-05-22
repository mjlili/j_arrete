package upem.jarret.ClientJarRet;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;

public class HTTPReader {

	private final Charset ASCII_CHARSET = Charset.forName("ASCII");
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
				throw new HTTPException("Connection with server is closed");
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
			throw new HTTPException("Connection with server is closed");
		}
		return buffer;
	}

	/**
	 * @return a ByteBuffer in write-mode containing a content read in chunks
	 *         mode
	 * @throws IOException
	 *             HTTPException if the connection is closed before the end of
	 *             the chunks if chunks are ill-formed
	 */

	public ByteBuffer readChunks() throws IOException {
		LinkedList<ByteBuffer> buffers = new LinkedList<ByteBuffer>();
		boolean isCompleted = false;
		while (!isCompleted) {
			String line = readLineCRLF();
			int chunkSize = Integer.parseInt(line, 16);
			if (chunkSize == 0) {
				isCompleted = true;
				continue;
			}
			ByteBuffer bufferReceived = ByteBuffer.allocate(chunkSize);
			bufferReceived.put(buff.flip());
			buff.compact();
			if (!readFully(bufferReceived, sc)) {
				throw new HTTPException("Connection with server is closed");
			}
			buffers.add(bufferReceived.flip());
			readLineCRLF();
		}
		int totalSize = buffers.stream().mapToInt(buffer -> buffer.remaining()).sum();
		System.err.println(totalSize);
		ByteBuffer finalBuffer = ByteBuffer.allocate(totalSize);
		for (ByteBuffer buffer : buffers) {
			finalBuffer.put(buffer);
		}
		System.out.println(finalBuffer.toString());
		return finalBuffer;
	}

	public static void main(String[] args) throws IOException {
		Charset charsetASCII = Charset.forName("ASCII");
		String request = "GET / HTTP/1.1\r\n" + "Host: www.w3.org\r\n" + "\r\n";
		SocketChannel sc = SocketChannel.open();
		sc.connect(new InetSocketAddress("www.w3.org", 80));
		sc.write(charsetASCII.encode(request));
		ByteBuffer bb = ByteBuffer.allocate(50);
		HTTPReader reader = new HTTPReader(sc, bb);
		System.out.println(reader.readLineCRLF());
		System.out.println(reader.readLineCRLF());
		System.out.println(reader.readLineCRLF());
		sc.close();

		bb = ByteBuffer.allocate(50);
		sc = SocketChannel.open();
		sc.connect(new InetSocketAddress("www.w3.org", 80));
		reader = new HTTPReader(sc, bb);
		sc.write(charsetASCII.encode(request));
		System.out.println(reader.readHeader());
		sc.close();

		bb = ByteBuffer.allocate(50);
		sc = SocketChannel.open();
		sc.connect(new InetSocketAddress("www.w3.org", 80));
		reader = new HTTPReader(sc, bb);
		sc.write(charsetASCII.encode(request));
		HTTPHeader header = reader.readHeader();
		System.out.println(header);
		ByteBuffer content = reader.readBytes(header.getContentLength());
		content.flip();
		System.out.println(header.getCharset().decode(content));
		sc.close();

		bb = ByteBuffer.allocate(50);
		request = "GET / HTTP/1.1\r\n" + "Host: www.u-pem.fr\r\n" + "\r\n";
		sc = SocketChannel.open();
		sc.connect(new InetSocketAddress("www.u-pem.fr", 80));
		reader = new HTTPReader(sc, bb);
		sc.write(charsetASCII.encode(request));
		header = reader.readHeader();
		System.out.println(header);
		content = reader.readChunks();
		content.flip();
		System.out.println(header.getCharset().decode(content));
		sc.close();
	}
}
