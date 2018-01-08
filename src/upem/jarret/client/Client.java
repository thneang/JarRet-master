package upem.jarret.client;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.rmi.UnexpectedException;
import java.util.HashMap;

import upem.jarret.http.HTTPException;
import upem.jarret.http.HTTPHeader;
import upem.jarret.http.HTTPReader;
import upem.jarret.job.Task;
import upem.jarret.worker.Worker;
import upem.jarret.worker.WorkerFactory;
import util.JsonTools;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParseException;

public class Client {
	private static final Charset charsetASCII = Charset.forName("ASCII");
	private static final Charset charsetUTF8 = Charset.forName("utf-8");

	private final String id;
	private final InetSocketAddress sa;
	private final HashMap<String, Worker> workers = new HashMap<>();
	private SocketChannel sc;

	public Client(String id, String serverAddress, int port) throws IOException {
		this.id = id;
		sa = new InetSocketAddress(serverAddress, port);
	}

	/**
	 * Requests a task to do
	 * 
	 * @return task data
	 * @throws HTTPException 
	 * @throws IOException
	 */
	private Task requestTask() throws HTTPException, IOException {
		// send the request
		String request = "GET Task HTTP/1.1\r\n" + "Host: " + sa.getHostName() + "\r\n" + "\r\n";
		sc.write(charsetASCII.encode(request));

		// read the response
		ByteBuffer bb = ByteBuffer.allocate(50);
		HTTPReader reader = new HTTPReader(sc, bb);
		HTTPHeader header = reader.readHeader();
		
		if (header.getCode() == 400) {
			throw new IllegalArgumentException("Bad request: " + request);
		} else if (header.getCode() != 200) {
			throw new UnexpectedException("Wrong http code: " + header.getCode());
		}
		ByteBuffer content = reader.readBytes(header.getContentLength());

		// parse json
		content.flip();
		return Task.parseJSON(charsetUTF8.decode(content).toString());
	}

	private void checkCode() throws IOException {
		ByteBuffer bb = ByteBuffer.allocate(50);
		HTTPReader reader = new HTTPReader(sc, bb);
		HTTPHeader header = reader.readHeader();
		System.out.println("Answer from server : " + header.getCode());
		
		if(!(header.getCode() == 200)) {
			throw new IllegalArgumentException();
		} 
    }

	/**
	 * Tests if there are errors in the answer
	 * 
	 * @param answer String to test
	 * @return	the error message
	 * @throws JsonParseException if the parsing went wrong
	 * @throws IOException if something went wrong
	 */
	private String checkError(String answer) throws JsonParseException, IOException {
		if (answer == null) {
			return "Computation error";
		}

		if (!JsonTools.isJSON(answer)) {
			return "Answer is not valid JSON";
		}

		if (JsonTools.isNested(answer)) {
			return "Answer is nested";
		}

		return null;
	}

	/**
	 * Creates the string answer to POST
	 * 
	 * @param task the task the client work on
	 * @param answer the answer the worker calculates
	 * @param error the error message is there is one
	 * @return the request
	 * @throws IOException if something went wrong
	 */
	private ByteBuffer createRequest(Task task, String answer, String error) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		JsonFactory jf = new JsonFactory();
		JsonGenerator jg = jf.createGenerator(baos);

		jg.writeStartObject();

		jg.writeStringField("JobId", String.valueOf(task.getJobId()));
		jg.writeStringField("WorkerVersion", task.getWorkerVersion());
		jg.writeStringField("WorkerURL", task.getWorkerURL());
		jg.writeStringField("WorkerClassName", task.getWorkerClassName());
		jg.writeStringField("Task", String.valueOf(task.getTask()));
		jg.writeStringField("ClientId", id);

		if (error == null) {
			jg.writeFieldName("Answer");
			jg.writeRawValue(answer);
		} else {
			jg.writeStringField("Error", error);
		}

		jg.writeEndObject();
		jg.close();
		
		String json = baos.toString();
		ByteBuffer jsonBuffer = charsetUTF8.encode(json);
		
		return jsonBuffer;
	}
	
	/**
	 * Sends the answer to the server
	 * 
	 * @param task
	 * @param answer
	 * @throws IOException
	 */
	private void sendAnswer(Task task, String answer) throws IOException {
		ByteBuffer jsonBuffer = createRequest(task, answer, checkError(answer));
		ByteBuffer content = ByteBuffer.allocate(Long.BYTES + Integer.BYTES);
		
		content.putLong(task.getJobId()).putInt(task.getTask());
		content.flip();
		if(content.remaining() + jsonBuffer.remaining() > 4096) {
			jsonBuffer = createRequest(task, answer, "Too Long");
		}
		
		int contentLength = content.remaining() + jsonBuffer.remaining();
		String header = "POST Answer HTTP/1.1\r\nHost: " + sa.getHostName() + "\r\nContent-Type: application/json\r\nContent-Length: " + contentLength + "\r\n\r\n";
		
		ByteBuffer bb = charsetASCII.encode(header);
		
		sc.write(bb);
		sc.write(content);
		sc.write(jsonBuffer);
	}

	/**
	 * Interacts with the server
	 * 
	 * @throws IOException if something went wrong
	 * @throws InterruptedException if the something is interrupted
	 * @throws ClassNotFoundException if the class was not found
	 * @throws IllegalAccessException if the class is not accessible
	 * @throws InstantiationException if the instantiation went wrong
	 */
	public void interact() throws IOException, InterruptedException, ClassNotFoundException, IllegalAccessException,
	        InstantiationException {
		Task task = new Task();
		Worker worker = null;
		do {
			connect();
			while (true) {
				try {
					System.out.println("Requesting task");
					task = requestTask();
				} catch(IllegalArgumentException e) {
					System.err.println(e.getMessage());
					task.setComeBackInSeconds(300);
				} catch(IOException e) {
					connect();
					continue;
				}
				try {
					Thread.sleep(task.getComeBackInSeconds());
				} catch (IllegalArgumentException e) {
					break;
				}
			}
			System.out.println("Task received: "+task.toJSON());
			System.out.println("Retrieving worker");
			if (worker == null ||  (task.getWorkerClassName() != worker.getClass().getName() && task.getWorkerVersion() != worker.getVersion())) {
				worker = workers.get(task.getWorkerClassName());
				if(worker == null || task.getWorkerVersion() != worker.getVersion()) {
					worker = WorkerFactory.getWorker(task.getWorkerURL(), task.getWorkerClassName());
					workers.put(task.getWorkerClassName(), worker);
				}
			}
			String answer;
			try {
				System.out.println("Starting computation");
				answer = worker.compute(task.getTask());
			} catch (Exception e) {
				answer = null;
			}
			while(true) {
				while(true) {
					try{
						System.out.println("Sending answer");
						sendAnswer(task, answer);
						break;
					} catch(IOException e) {
						connect();
					}
				}
				try{
					checkCode();
					break;
				} catch(IOException e) {
					//connect();
				} catch(IllegalArgumentException e) {
					System.out.println("Server does not reply with 200");
				}
			}
			try{
				sc.close();
			} catch(Exception e) {
				//
			}
			System.out.println("\n--------------------------------------\n");
		} while (true);
	}

	private void connect() {
		try{
			sc.close();
		} catch(Exception e) {
			//
		}
		while(true) {
			System.out.println("Trying to connect with server...");
			try {
				sc = SocketChannel.open();
				sc.connect(sa);
				return;
			} catch(ConnectException e) {
				//
			} catch(IOException e) {
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

	private static void usage() {
		System.out.println("ClientJarRet clientId serverAddress serverPort");
	}
	
	public static void main(String[] args) throws JsonParseException, IOException, ClassNotFoundException,
	        IllegalAccessException, InstantiationException, InterruptedException {
		
		if (args.length != 3) {
			usage();
			return;
		}
		
		Client client = new Client(args[0], args[1], Integer.valueOf(args[2]));
		client.interact();
	}

}