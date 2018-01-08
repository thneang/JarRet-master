package upem.jarret.job;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

public class Task {
	private String _JobId;
	private String _WorkerVersion;
	private String _WorkerURL;
	private String _WorkerClassName;
	private String _Task;
	private int _ComeBackInSeconds = 300;

	public Task() {}
	
	public Task(String jobId, String workerVersion, String workerURL, String workerClassName, int task) {
		this._JobId = jobId;
		this._WorkerVersion = workerVersion;
		this._WorkerURL = workerURL;
		this._WorkerClassName = workerClassName;
		this._Task = String.valueOf(task);
		_ComeBackInSeconds = -1;
	}
	
	/**
	 * Functions used by the JSON parser
	 */
	
	public long getJobId() {
		return Long.parseLong(_JobId);
	}

	public String getWorkerVersion() {
		return _WorkerVersion;
	}

	public String getWorkerURL() {
		return _WorkerURL;
	}

	public String getWorkerClassName() {
		return _WorkerClassName;
	}

	public int getTask() {
		return Integer.parseInt(_Task);
	}

	public int getComeBackInSeconds() {
		return _ComeBackInSeconds;
	}

	public void setJobId(String jobId) {
		_JobId = jobId;
	}

	public void setWorkerVersion(String wv) {
		_WorkerVersion = wv;
	}

	public void setWorkerURL(String wURL) {
		_WorkerURL = wURL;
	}

	public void setWorkerClassName(String wcn) {
		_WorkerClassName = wcn;
	}

	public void setTask(String t) {
		_Task = t;
	}

	public void setComeBackInSeconds(int cbis) {
		_ComeBackInSeconds = cbis;
	}
	
	public boolean checkFull() {
		return _JobId != null && _WorkerVersion != null && _WorkerClassName != null && _WorkerURL != null && _Task != null;
	}

	/**
	 * Parses buffer content with Jackson Streaming API
	 * 
	 * @param content JSOn to parse
	 * @return task data parsed
	 * @throws IOException
	 * @throws JsonParseException
	 */
	public static Task parseJSON(String json) throws JsonParseException, IOException {
		Task task = new Task();
		JsonFactory jf = new JsonFactory();
		JsonParser jp = jf.createParser(json);
		jp.nextToken();
		while (jp.nextToken() != JsonToken.END_OBJECT) {
			String fieldname = jp.getCurrentName();
			jp.nextToken();
			if ("ComeBackInSeconds".equals(fieldname)) {
				task.setComeBackInSeconds(jp.getIntValue());
			} else if ("JobId".equals(fieldname)) {
				task.setJobId(jp.getText());
				task.setComeBackInSeconds(-1);
			} else if ("WorkerVersion".equals(fieldname)) {
				task.setWorkerVersion(jp.getText());
			} else if ("WorkerURL".equals(fieldname)) {
				task.setWorkerURL(jp.getText());
			} else if ("WorkerClassName".equals(fieldname)) {
				task.setWorkerClassName(jp.getText());
			} else if ("Task".equals(fieldname)) {
				task.setTask(jp.getText());
			} else {
				throw new IllegalStateException("Unrecognized field name: " + fieldname);
			}
		}
		jp.close();
		return task;
	}
	
	/**
	 * Creates a JSOn String 
	 * 
	 * @return
	 * @throws IOException
	 */
	public String toJSON() throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		JsonFactory jf = new JsonFactory();
		JsonGenerator jg = jf.createGenerator(baos);

		jg.writeStartObject();

		jg.writeStringField("JobId", _JobId);
		jg.writeStringField("WorkerVersion", _WorkerVersion);
		jg.writeStringField("WorkerURL", _WorkerURL);
		jg.writeStringField("WorkerClassName", _WorkerClassName);
		jg.writeStringField("Task", _Task);

		jg.writeEndObject();
		jg.close();
		
		return baos.toString();
	}
}
