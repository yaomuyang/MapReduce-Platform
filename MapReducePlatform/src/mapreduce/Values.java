package mapreduce;

import java.util.List;

public class Values {

	private List<Integer> valueList;
	private int index;
	
	public void setValueList(List<Integer> valueList){
		this.valueList = valueList;
	}
	
	public List<Integer> getValueList(){
		return this.valueList;
	}
	
	public boolean hasNext(){
		if(valueList.size()==0 || index == valueList.size() )
		{
			return false;
		}
		return true;
	}
	
	public int getNextValue(){
		//System.out.println("Returning value["+index+"]");
		int returnValue = valueList.get(index);
		index++;
		return returnValue;
	}
}