package upem.jarret.server;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

public class Parser {

	public static List<HashMap<String,Object>> parseJSON(String filePath,String root) throws JsonProcessingException, IOException {
		
		ObjectMapper mapper = new ObjectMapper(); 
	    TypeReference<HashMap<String,String>> typeRef = new TypeReference<HashMap<String,String>>() {};
	    
	    File jsonFile = new File(filePath);
	    JsonNode rootNode = mapper.readTree(jsonFile);
	    
	    ArrayNode slaidsNode = (ArrayNode) rootNode.get(root);
	    Iterator<JsonNode> slaidsIterator = slaidsNode.elements();
	    List<HashMap<String,Object>> workersList = new ArrayList<>();
	    while (slaidsIterator.hasNext()) {	    	
	    	HashMap<String,Object> worker = mapper.convertValue(slaidsIterator.next(),typeRef);
		    workersList.add(worker);
	    }
	    return workersList;
	}
}
