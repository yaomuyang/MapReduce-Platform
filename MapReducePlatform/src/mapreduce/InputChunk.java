package mapreduce;

/* 
 * The input to the mapper function is abstracted in the form of an InputChunk 
 */
public class InputChunk {

	private byte[] input;
	
	public InputChunk(byte[] b){
		this.input = b;
	}
	
	public String toString(){
		String inputstring = new String(input);
		return inputstring;
	}
}