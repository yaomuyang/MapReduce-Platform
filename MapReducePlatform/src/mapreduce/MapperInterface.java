package mapreduce;

public interface MapperInterface {
	//Map function, to be overwrite
	public void map(Key key,InputChunk inputChunk, Output output) throws Exception;
}