package mapreduce;

public interface ReducerInterface {
	//Reduce function, to be overwrite
	public void reduce(Key keys,Values values,Output output) throws Exception;
}