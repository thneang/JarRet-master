package upem.jarret.server;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import upem.jarret.http.HTTPReaderServer;

public class Attachment {
	private HTTPReaderServer reader;
	private boolean requestingTask = false;
	private boolean sendingPost = false;
	private boolean readingRequest = true;
	private boolean parsingRequest = false;
	private boolean readingAnswer = false;
	private String request = null;
	private String answer = null;
	private final ByteBuffer in;
	private int contentLength;

	public Attachment(SocketChannel sc) {
		in = ByteBuffer.allocate(1024);
		reader = new HTTPReaderServer(sc, ByteBuffer.allocate(50), in);
	}

	/**
	 * Set requestingTask to true
	 */
	public void requestTask() {
		setRequestingTask(true);
	}

	/**
	 * 
	 * @return the value of requestingTask
	 */
	public boolean isRequestingTask() {
		return requestingTask;
	}

	/**
	 * Set the value of requestingTask with the param requestingTask
	 * 
	 * @param requestingTask 
	 */
	void setRequestingTask(boolean requestingTask) {
		this.requestingTask = requestingTask;
	}

	/**
	 * Set answer with the value of answer
	 * 
	 * @param answer
	 */
	public void requestAnswer(String answer) {
		this.setAnswer(answer);
		setSendingPost(true);
	}

	/**
	 * Returns the value of sendingPost
	 * 
	 * @return
	 */
	public boolean isSendingPost() {
		return sendingPost;
	}

	/**
	 * Set the value of sendingPost
	 * 
	 * @param sendingPost
	 */
	private void setSendingPost(boolean sendingPost) {
		this.sendingPost = sendingPost;
	}

	/**
	 * Returns the reader
	 * 
	 * @return
	 */
	public HTTPReaderServer getReader() {
		return reader;
	}

	/**
	 * Returns the answer
	 * 
	 * @return
	 */
	public String getAnswer() {
		return answer;
	}

	/**
	 * Set the value of answer
	 * 
	 * @param answer
	 */
	private void setAnswer(String answer) {
		this.answer = answer;
	}

	/**
	 * Set sendingPost to false and create a new HTTPReader for the next task
	 * 
	 * @param sc the SocketChannel used by the HTTPReader
	 */
	public void clean(SocketChannel sc) {
		setSendingPost(false);
		in.clear();
		reader = new HTTPReaderServer(sc, ByteBuffer.allocate(50), in);
	}

	public ByteBuffer getIn() {
		return in;
	}
	
	public boolean isReadingRequest() {
		return readingRequest;
	}

	public void setReadingRequest(boolean b) {
		readingRequest = b;
	}

	public String getRequest() {
		return request;
	}
	
	public void setRequest(String request) {
		this.request = request;
	}

	public boolean isParsingRequest() {
		return parsingRequest;
	}

	public void setParsingRequest(boolean b) {
		parsingRequest = b;
	}

	public boolean isReadingAnswer() {
		return readingAnswer;
	}

	public void setReadingAnswer(boolean b) {
		this.readingAnswer = b;
	}

	public int getContentLength() {
		return contentLength;
	}

	public void setContentLength(int length) {
		contentLength = length;
	}

}
