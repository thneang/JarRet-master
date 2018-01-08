package upem.jarret.http;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class HTTPReaderServer extends HTTPReader {
	private final ByteBuffer in;
	private StringBuilder currentLine;
	private ByteBuffer currentAnswer;

	public HTTPReaderServer(SocketChannel sc, ByteBuffer buff, ByteBuffer in) {
		super(sc, buff);
		this.in = in;
		currentLine = new StringBuilder();
	}

	/**
	 * @return The ASCII string terminated by CRLF
	 * <p>
	 * The method assume that buff is in write mode and leave it in write-mode
	 * The method never reads from the socket as long as the buffer is not empty
	 * @throws IOException HTTPException if the connection is closed before a line could be read
	 */
	@Override
	public String readLineCRLF() throws IOException {
		in.flip();

		boolean lastChar = false;
		StringBuilder line = new StringBuilder();
		do{
			if(!in.hasRemaining()) {
				in.clear();
				currentLine.append(line);
				throw new IllegalStateException();
			}
			char b = (char)in.get();
			if(b == '\n' && lastChar) {
				in.compact();
				String res = currentLine.append(line).toString();
				currentLine = new StringBuilder();
				return res;
			}
			if(b == '\r') {
				lastChar = true;
			}
			else {
				if(lastChar) {
					line.append("\r");
				}
				lastChar = false;
				line.append(b);
			}
		} while(true);
	}

	/**
	 * @param size
	 * @return a ByteBuffer in write-mode containing size bytes read on the socket
	 * @throws IOException HTTPException is the connection is closed before all bytes could be read
	 */
	@Override
	public ByteBuffer readBytes(int size) throws IOException {
		if(currentAnswer == null || currentAnswer.capacity() != size) {
			currentAnswer = ByteBuffer.allocate(size);
		}
		in.flip();
		currentAnswer.put(in);
		if(currentAnswer.position() != currentAnswer.capacity()) {
			in.clear();
			throw new IllegalStateException();
		}
		
		return currentAnswer;
	}

}
