package upem.jarret.server;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Optional;

public class ManageServer {

	private static final Charset UTF8_CHARSET = Charset.forName("UTF-8");

	public static void writeAnswerToFile(ByteBuffer byteBuffer,String jobFileName) throws FileNotFoundException {
		Optional<String> answer = readByteBuffer(byteBuffer);
		if(answer.isPresent()) {
			writeStringToFile(answer.get(), jobFileName);
		}else {
			System.err.println("Couldn't decode answer.");
		}
	}
	
	private static Optional<String> readByteBuffer(ByteBuffer byteBuffer){
		return Optional.of(UTF8_CHARSET.decode(byteBuffer).toString());
	}
	
	private static void writeStringToFile(String text,String jobFileName) throws FileNotFoundException {
		//*Recreate file if exists*/
		try(  PrintWriter out = new PrintWriter(jobFileName)){
		    out.println(text);
		}
	}
}
