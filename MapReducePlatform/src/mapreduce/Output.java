package mapreduce;

import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.Serializable;
import java.util.List;

/* 
 * the mapper function writes key value pairs to output via the Output object. Every mapper maintains a copy of this Output
 */
@SuppressWarnings("serial")
public class Output implements Serializable {

	/* the list of intermediate files to be set by the Resource Manager */
	private String[] intermediateFileNames;
	
	/* declared transient */
	transient FileOutputStream[] fos;
	transient DataOutputStream[] dos;
	
	/* the resource manager initializes the Output Object for the mappers of an application with the intermediateFileNames array */
	public Output(List<String> intermediateFileNames){
		int fileNameIndex = 0;
		
		this.intermediateFileNames = new String[intermediateFileNames.size()];
		
		for(String intermediateFileName : intermediateFileNames){
			this.intermediateFileNames[fileNameIndex++] = intermediateFileName;
		}
	}
	
	public String[] getFileName(){
		return this.intermediateFileNames;
	}

	/* we open the file handlers for the intermediate files here and each mapper can operate with the open file handle */
	public void openFileHandles()
	{
		try{
			fos = new FileOutputStream[this.intermediateFileNames.length];
			dos = new DataOutputStream[this.intermediateFileNames.length];
			for(int i=0;i<this.intermediateFileNames.length;i++){
				fos[i] = new FileOutputStream("IM/"+this.intermediateFileNames[i]);
				dos[i] = new DataOutputStream(fos[i]);
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/* function used in the mapper function to collect key-value pairs and write to intermediate files */
	public void write(Key keyObject, int valueObj) throws Exception{
		
		String key = keyObject.getKey();
		String value = String.valueOf(valueObj);
		/* determine which intermediate file the key is to be hashed to */
		int hashValue = (key.hashCode() % this.intermediateFileNames.length + this.intermediateFileNames.length) % this.intermediateFileNames.length;
		
		/* write the key-value pair to that intermediate file */		
		/*To eliminate the "\n", an offset and a random access file can be used. necessary?*/
		/* each key-value pair is recorded in a new line in the intermediate file */		
		String keyValuePair = key+" "+value+"\n";
		byte[] b = new byte[10];
		b = keyValuePair.getBytes();
		dos[hashValue].write(b);
	}
	
	/* we close the file handles of the intermediate files */
	public void closeFileHandles() throws Exception{
		for(int i=0;i<this.intermediateFileNames.length;i++){
			this.dos[i].close();
			this.fos[i].close();
		}		
	}
}