package upem.jarret.server;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class HTTPServerReader {

	private final ByteBuffer bufferIn;
	private StringBuilder currentLine;
	private ByteBuffer currentAnswer;

	public HTTPServerReader(SocketChannel sc, ByteBuffer bufferIn) throws IOException {
		this.bufferIn = bufferIn;
		currentLine = new StringBuilder();
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
		bufferIn.flip();
		boolean lastChar = false;
		StringBuilder line = new StringBuilder();
		while (true) {
			if (!bufferIn.hasRemaining()) {
				bufferIn.clear();
				currentLine.append(line.toString());
				// Pour dire que la lecture de la ligne n'a rien donné
				throw new IllegalStateException();
			}
			char b = (char) bufferIn.get();
			if (b == '\n' && lastChar) {
				bufferIn.compact();
				String res = currentLine.append(line).toString();
				currentLine = new StringBuilder();
				return res;
			}
			if (b == '\r') {
				lastChar = true;
			} else {
				if (lastChar) {
					line.append("\r");
				}
				lastChar = false;
				line.append(b);
			}
		}
	}

	/**
	 * @param contentLength
	 * @return a ByteBuffer in write-mode containing size bytes read on the
	 *         socket
	 * @throws IOException
	 *             HTTPException is the connection is closed before all bytes
	 *             could be read
	 */
	public ByteBuffer readBytes(int contentLength) throws IOException {
		if (currentAnswer == null || currentAnswer.capacity() != contentLength) {
			currentAnswer = ByteBuffer.allocate(contentLength);
		}
		bufferIn.flip();
		currentAnswer.put(bufferIn);
		if (currentAnswer.position() != currentAnswer.capacity()) {
			bufferIn.clear();
			throw new IllegalStateException();
		}
		return currentAnswer;
	}
}
