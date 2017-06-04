package utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class JsonUtils {

	/**
	 * Converts a String to an ObjectNode
	 * 
	 * @param content
	 *            String contains the json content
	 * @return ObjectNode represents the json content
	 * @throws JsonProcessingException
	 * @throws IOException
	 */
	public static ObjectNode fromStringToJson(String content) throws JsonProcessingException, IOException {
		ObjectMapper mapper = new ObjectMapper();
		JsonNode node = mapper.readTree(content);
		ObjectNode objectNode = (ObjectNode) node;
		return objectNode;
	}

	/**
	 * Parses a file to convert it to a String
	 * 
	 * @param path
	 *            file path to parse
	 * @return String contains the file content
	 * @throws IOException
	 */
	public static String parseJsonFile(String path) throws IOException {
		Path filePath = Paths.get(path);
		BufferedReader reader = Files.newBufferedReader(filePath);
		StringBuilder sb = new StringBuilder();
		reader.lines().forEach(line -> sb.append(line));
		reader.close();
		return sb.toString();
	}

	/**
	 * Tests if the string is in json
	 * 
	 * @param string
	 *            to test
	 * @return true if the string is in json, false otherwise
	 * @throws IOException
	 *             if something went wrong
	 */
	public static boolean isValidJsonString(String jsonString) {
		try {
			final ObjectMapper mapper = new ObjectMapper();
			mapper.readTree(jsonString);
			return true;
		} catch (IOException e) {
			return false;
		}
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
	public static boolean jsonContainsObjectField(ObjectNode objectNode) {
		Iterator<JsonNode> iterator = objectNode.elements();
		while (iterator.hasNext()) {
			JsonNode node = iterator.next();
			if (node.getNodeType() == JsonNodeType.OBJECT) {
				return true;
			}
		}
		return false;
	}

}
