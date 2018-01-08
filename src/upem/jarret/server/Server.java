package upem.jarret.server;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
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
import java.util.ArrayDeque;
import java.util.Objects;
import java.util.Scanner;
import java.util.Set;

import upem.jarret.http.HTTPReaderServer;
import upem.jarret.job.Job;
import util.JsonTools;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

public class Server {
	private static final String HTTP_1_1_200_OK = "HTTP/1.1 200 OK\r\n\r\n";
	static final Charset charsetASCII = Charset.forName("ASCII");
	static final Charset charsetUTF8 = Charset.forName("utf-8");
	static final String badRequest = "HTTP/1.1 400 Bad Request\r\n\r\n";

	private final ServerSocketChannel ssc;
	private final Selector selector;
	private final Set<SelectionKey> selectedKeys;

	private final String logPath;
	private final String answersPath;
	private final long maxFileSize;
	private final int comeBackInSeconds;
	private final ArrayDeque<Job> jobs = new ArrayDeque<Job>();
	private final Object clientMonitor = new Object();
	//private final PrintWriter log;

	private boolean shutdown = false;
	private SelectionKey acceptKey;
	private int nbClients = 0;
	private int nbAnswers = 0;

	private final Thread consoleThread = new Thread(() -> {
		try (Scanner scanner = new Scanner(System.in)) {
			while (scanner.hasNextLine()) {
				switch (scanner.nextLine()) {
				case "SHUTDOWN":
					shutdown();
					break;
				case "SHUTDOWN NOW":
					shutdownNow();
					break;
				case "INFO":
					info();
					break;
				default:
					System.out.println("WRONG COMMAND");
					break;
				}
			}
		}
	});

	private Server(int port, String logPath, String answersPath, long maxFileSize, int comeBackInSeconds)
			throws IOException {
		this.logPath = logPath;
		this.answersPath = answersPath;
		this.maxFileSize = maxFileSize;
		this.comeBackInSeconds = comeBackInSeconds;

		ssc = ServerSocketChannel.open();
		ssc.bind(new InetSocketAddress(port));
		selector = Selector.open();
		selectedKeys = selector.selectedKeys();
	}

	/**
	 * Prints info
	 */
	private void info() {
		System.out.println("INFO");
		synchronized (clientMonitor) {
			System.out.println("Connected clients: "+nbClients);
		}
		System.out.println("Next task: - jobId: "+jobs.getFirst().getJobId()+" - task: "+jobs.getFirst().getCurrentTask());
		System.out.println("Answers received: "+nbAnswers);
	}

	/**
	 * Close the accepting key
	 */
	private void shutdown() {
		System.out.println("SHUTDOWN");

		try {
			close(acceptKey);
		} catch (IOException e) {
			//
		}
		shutdown = true;
	}

	/**
	 * Close all the keys
	 */
	private void shutdownNow() {
		System.out.println("SHUTDOWN NOW");
		try {
			close(acceptKey);
		} catch (IOException e) {
			//
		}
		for (SelectionKey key : selector.keys()) {
			try {
				close(key);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		shutdown = true;
	}

	/**
	 * Launches the server
	 * 
	 * @throws IOException
	 */
	public void launch() throws IOException {
		consoleThread.setDaemon(true);
		consoleThread.start();

		ssc.configureBlocking(false);
		acceptKey = ssc.register(selector, SelectionKey.OP_ACCEPT);
		saveLog("Server launched on port " + ssc.getLocalAddress());
		Set<SelectionKey> selectedKeys = selector.selectedKeys();

		loadJobs();

		while (!selector.keys().isEmpty() || !shutdown) {
			selector.select(300);
			processSelectedKeys();
			selectedKeys.clear();
		}
	}

	/**
	 * Loads the job from the config file
	 * 
	 * @throws JsonParseException
	 * @throws IOException
	 */
	private void loadJobs() throws JsonParseException, IOException {
		Path jobsConfigPath = Paths.get("config/JarRetJobs.json");

		JsonFactory jf = new JsonFactory();
		JsonParser jp = jf.createParser(Files.newBufferedReader(jobsConfigPath));

		JsonToken current = jp.nextToken();
		while (current != null) {
			switch (current) {
			case START_OBJECT:
				Job job = Job.parseJSON(jp);
				for (int i = 0; i < Integer.parseInt(job.getJobPriority()); i++) {
					jobs.add(job);
				}
				break;
			default:
				break;
			}
			current = jp.nextToken();
		}

		for (Job job : jobs) {
			System.out.println(job);
		}
	}

	/**
	 * Process the keys
	 * 
	 * @throws IOException
	 */
	private void processSelectedKeys() throws IOException {
		for (SelectionKey key : selectedKeys) {

			if (key.isValid() && key.isAcceptable()) {
				try {
					doAccept(key);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			if (key.isValid() && key.isWritable()) {
				try {
					doWrite(key);
				} catch (IOException e) {
					SocketChannel sc = (SocketChannel) key.channel();
					saveLog("Connection lost with client "+sc.getRemoteAddress());
					close(key);
					synchronized (clientMonitor) {
						nbClients--;
					}
				}
			}
			if (key.isValid() && key.isReadable()) {
				try {
					doRead(key);
				} catch (IOException e) {
					SocketChannel sc = (SocketChannel) key.channel();
					saveLog("Connection lost with client "+sc.getRemoteAddress());
					close(key);
					synchronized (clientMonitor) {
						nbClients--;
					}
				}
			}
		}
	}

	/**
	 * Accepts a key
	 * 
	 * @param key
	 * @throws IOException
	 */
	private void doAccept(SelectionKey key) throws IOException {
		SocketChannel sc = ssc.accept();
		if (sc == null) {
			return;
		}
		sc.configureBlocking(false);
		sc.register(selector, SelectionKey.OP_READ, new Attachment(sc));
		saveLog("New connection from " + sc.getRemoteAddress());
		synchronized(clientMonitor) {
			nbClients++;
		}
	}

	/**
	 * reads from the channel of the key
	 * 
	 * @param key
	 * @throws IOException
	 */
	private void doRead(SelectionKey key) throws IOException {
		SocketChannel sc = (SocketChannel) key.channel();
		Attachment attachment = (Attachment) key.attachment();
		HTTPReaderServer reader = attachment.getReader();

		sc.read(attachment.getIn());

		if(attachment.isReadingRequest()) {
			try{
				attachment.setRequest(reader.readLineCRLF());
				attachment.setReadingRequest(false);
			} catch(IllegalStateException e) {
				return;
			}
		}

		try {
			parseRequest(attachment, sc);
		} catch(IllegalStateException e) {
			System.out.println("resuqesting task !");
			return;
		} catch (Exception e) {
			sc.write(charsetUTF8.encode(badRequest));
			return;
		}

		key.interestOps(SelectionKey.OP_WRITE);
	}

	/**
	 * Parses a request wich the server received
	 * 
	 * @param request
	 * @param attachment
	 * @param sc
	 * @throws IOException
	 */
	private void parseRequest(Attachment attachment, SocketChannel sc) throws IOException {
		String request = attachment.getRequest();
		String firstLine = request.split("\r\n")[0];
		String[] token = firstLine.split(" ");
		String cmd = token[0];
		String requested = token[1];
		String protocol = token[2];

		if (cmd.equals("GET") && requested.equals("Task") && protocol.equals("HTTP/1.1")) {
			if(!attachment.isParsingRequest()){
				saveLog("Client "+sc.getRemoteAddress()+ " is requesting a task");
			}
			attachment.requestTask();
			attachment.setParsingRequest(true);
			if (attachment.isParsingRequest()) { 
				while(!attachment.getReader().readLineCRLF().equals("")){/** read useless parameters og GET request **/}
				attachment.setParsingRequest(false);
			}
		} else if (cmd.equals("POST") && requested.equals("Answer") && protocol.equals("HTTP/1.1")) {
			if(!attachment.isParsingRequest()) {
				saveLog("Client "+sc.getRemoteAddress() + " is posting an answer");
			}
			attachment.setParsingRequest(true);
			String answer = parsePOST(attachment);
			Objects.requireNonNull(answer);
			attachment.requestAnswer(answer);
			attachment.setParsingRequest(false);
		} else {
			throw new IllegalArgumentException();
		}
	}

	/**
	 * Parses a POST request
	 * 
	 * @param attachment
	 * @return
	 * @throws IOException
	 */
	private String parsePOST(Attachment attachment) throws IOException {
		HTTPReaderServer reader = attachment.getReader();
		if(!attachment.isReadingAnswer()) {
			String line;
			while (!(line = reader.readLineCRLF()).equals("")) {
				String[] token = line.split(": ");
				if (token[0].equals("Content-Length")) {
					attachment.setContentLength(Integer.parseInt(token[1]));
				}
				if (token[0].equals("Content-Type")) {
					if (!token[1].equals("application/json")) {
						throw new IllegalArgumentException();
					}
				}
			}
			attachment.setReadingAnswer(true);
		}
		ByteBuffer bb = reader.readBytes(attachment.getContentLength());
		attachment.setReadingAnswer(false);
		bb.flip();
		long jobId = bb.getLong();
		int task = bb.getInt();
		String answer = charsetUTF8.decode(bb).toString();
		if (answer != null && JsonTools.isJSON(answer)) {
			saveAnswer(jobId, task, answer);
			nbAnswers++;
		}

		return answer;
	}

	/**
	 * Saves the String log into the log file
	 * 
	 * @param log
	 */
	private void saveLog(String log) {
		System.out.println(log);
		Path logFilePath = Paths.get(logPath+"log");

		try (BufferedWriter writer = Files.newBufferedWriter(logFilePath, StandardOpenOption.APPEND,
				StandardOpenOption.CREATE); PrintWriter outLog = new PrintWriter(writer)) {
			outLog.println(log);
		} catch (IOException e) {
			System.err.println(e);
		}
	}

	/**
	 * Saves the answer into the answer file
	 * 
	 * @param jobId
	 * @param task
	 * @param answer
	 * @throws IOException 
	 */
	private void saveAnswer(long jobId, int task, String answer) throws IOException {
		int fileNumber = 1;
		long size = 0;
		Path answerFilePath;
		answer += '\n';

		do {
			answerFilePath = Paths.get(answersPath + jobId + "_" + fileNumber++);
			if (Files.exists(answerFilePath, LinkOption.NOFOLLOW_LINKS)) {
				size = Files.size(answerFilePath);
			} else {
				break;
			}
		} while (size > maxFileSize);

		try (BufferedWriter writer = Files.newBufferedWriter(answerFilePath, StandardOpenOption.APPEND,
		        StandardOpenOption.CREATE); PrintWriter out = new PrintWriter(writer)) {
			out.println(answer);
		} catch (IOException e) {
			System.err.println(e);
		}
	}

	/**
	 * Creates a new server
	 * 
	 * @return
	 * @throws JsonParseException
	 * @throws IOException
	 */
	private static Server create() throws JsonParseException, IOException {
		Path serverConfigPath = Paths.get("config/JarRetConfig.json");

		int port = 8080;
		String logPath = "log/";
		String answersPath = "answers/";
		long maxFileSize = 0;
		int comeBackInSeconds = 300;

		JsonFactory jf = new JsonFactory();
		JsonParser jp = jf.createParser(Files.newBufferedReader(serverConfigPath));
		jp.nextToken();
		while (jp.nextToken() != JsonToken.END_OBJECT) {
			String fieldName = jp.getCurrentName();
			jp.nextToken();
			switch (fieldName) {
			case "Port":
				port = jp.getIntValue();
				break;
			case "LogDirectory":
				logPath = jp.getText();
				break;
			case "AnswersDirectory":
				answersPath = jp.getText();
				break;
			case "MaxFileSize":
				maxFileSize = jp.getLongValue();
				break;
			case "ComeBackInSeconds":
				comeBackInSeconds = jp.getIntValue();
				break;
			default:
				System.err.println("Unknown Field");
			}
		}

		return new Server(port, logPath, answersPath, maxFileSize, comeBackInSeconds);

	}

	/**
	 * Sends the task to the client
	 * 
	 * @param sc
	 * @throws IOException
	 */
	private void sendTask(SocketChannel sc) throws IOException {
		Job job = jobs.poll();
		ByteBuffer jsonBuffer;
		if (job == null) {
			jsonBuffer = charsetUTF8.encode("\"ComeBackInSeconds\":" + comeBackInSeconds);
		} else {
			while (job.isFinished()) {
				job = jobs.poll();
			}
			String json = job.nextTask().toJSON();
			jsonBuffer = Server.charsetUTF8.encode(json);
		}

		String header = "HTTP/1.1 200 OK\r\n" + "Content-Type: application/json; charset=utf-8\r\n"
				+ "Content-Length: " + jsonBuffer.remaining() + "\r\n\r\n";
		ByteBuffer headerBuffer = Server.charsetUTF8.encode(header);

		while (headerBuffer.hasRemaining()) {
			sc.write(headerBuffer);
		}

		while (jsonBuffer.hasRemaining()) {
			sc.write(jsonBuffer);
		}
		if (!job.isFinished()) {
			jobs.addLast(job);
		}
	}

	/**
	 * Write on the channel of the key
	 * 
	 * @param key
	 * @throws IOException
	 */
	private void doWrite(SelectionKey key) throws IOException {
		Attachment attachment = (Attachment) key.attachment();

		if (attachment.isRequestingTask()) {
			attachment.setRequestingTask(false);
			sendTask((SocketChannel) key.channel());
			key.interestOps(SelectionKey.OP_READ);
		} else if (attachment.isSendingPost()) {
			sendCheckCode(key);
			key.interestOps(SelectionKey.OP_READ);
		}

		attachment.setReadingRequest(true);
	}

	/**
	 * Sends the check code to the client
	 * 
	 * @param key
	 * @throws IOException
	 */
	private void sendCheckCode(SelectionKey key) throws IOException {
		Attachment attachment = (Attachment) key.attachment();
		SocketChannel sc = (SocketChannel) key.channel();
		String answer = attachment.getAnswer();
		if (answer == null) {
			throw new IllegalArgumentException("No answer");
		}
		if (JsonTools.isJSON(answer)) {
			sc.write(Server.charsetUTF8.encode(HTTP_1_1_200_OK));
		} else {
			sc.write(Server.charsetUTF8.encode(badRequest));
		}

		attachment.clean(sc);
	}

	/**
	 * Close the key
	 * 
	 * @param key
	 * @throws IOException
	 */
	private void close(SelectionKey key) throws IOException {
		try{
			key.channel().close();
			key.cancel();
		} catch(Exception e) {
			//
		}
	}

	private static void usage() {
		System.out.println("ServerJarRet");
	}
	
	public static void main(String[] args) throws NumberFormatException, IOException {
		
		if (args.length != 0) {
			usage();
			return;
		}
		
		Server.create().launch();
	}

}
