package utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class JsonTools {

	/**
	 * Tests if the string is in json
	 * 
	 * @param string
	 *            to test
	 * @return true if the string is in json, false otherwise
	 * @throws IOException
	 *             if something went wrong
	 */
	public static boolean isJSON(String string) throws IOException {
		JsonFactory jf = new JsonFactory();
		JsonParser jp = jf.createParser(string);
		try {
			jp.nextToken();
			while (jp.nextToken() != JsonToken.END_OBJECT) {
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
	 * @param json
	 *            the string to test
	 * @return true if the string is nested, false otherwise
	 * @throws JsonParseException
	 *             if the parsing went wrong
	 * @throws IOException
	 *             if something went wrong
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

	public static ObjectNode fromStringToJson(String content) throws JsonProcessingException, IOException {
		ObjectMapper mapper = new ObjectMapper();
		JsonNode node = mapper.readTree(content);
		ObjectNode objectNode = (ObjectNode) node;
		return objectNode;
	}

	public static String parseJsonFile(String path) throws IOException {
		Path filePath = Paths.get(path);
		BufferedReader reader = Files.newBufferedReader(filePath);
		StringBuilder sb = new StringBuilder();
		reader.lines().forEach(line -> sb.append(line));
		reader.close();
		return sb.toString();
	}
}
