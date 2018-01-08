package util;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

public class JsonTools {

	/**
	 * Tests if the string is in json
	 * 
	 * @param string to test
	 * @return true if the string is in json, false otherwise
	 * @throws IOException if something went wrong
	 */
	public static boolean isJSON(String string) throws IOException {
		JsonFactory jf = new JsonFactory();
		JsonParser jp = jf.createParser(string);
		try {
			jp.nextToken();
			while(jp.nextToken() != JsonToken.END_OBJECT) {
				jp.nextToken();
			}
		} catch (JsonParseException jpe) {
			return false;
		}
		return true;
	}
	
	/**
	 * Tests if the string is nested
	 * 
	 * @param json the string to test
	 * @return true if the string is nested, false otherwise
	 * @throws JsonParseException if the parsing went wrong
	 * @throws IOException if something went wrong
	 */
	public static boolean isNested(String json) throws JsonParseException, IOException {
		JsonFactory jf = new JsonFactory();
		JsonParser jp = jf.createParser(json);
		jp.nextToken();
		
		while ((jp.nextToken()) != JsonToken.END_OBJECT) {
			if ((jp.nextToken()) == JsonToken.START_OBJECT) {
				return true;
			}
		}
		return false;
	}

}
