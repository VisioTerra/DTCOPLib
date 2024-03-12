package fr.visioterra.lib.image.dataBuffer;

/**
 * DataBuffer interface.
 * @author Grégory Mazabraud
 * 
 * <br>Changes :
 * <br>- 2013/11/20		|	Grégory Mazabraud		|	initial version
 */
public interface DataBuffer {

	public int size();
	
	public int getDataType();
	
	
	public int getInt(int index) throws ArrayIndexOutOfBoundsException;
	
	public long getLong(int index) throws ArrayIndexOutOfBoundsException;
	
	public float getFloat(int index) throws ArrayIndexOutOfBoundsException;
	
	public double getDouble(int index) throws ArrayIndexOutOfBoundsException;
	
	public Object getObject(int index) throws ArrayIndexOutOfBoundsException;
	
	
	public void setInt(int index, int value) throws ArrayIndexOutOfBoundsException;
	
	public void setLong(int index, long value) throws ArrayIndexOutOfBoundsException;
	
	public void setFloat(int index, float value) throws ArrayIndexOutOfBoundsException;
	
	public void setDouble(int index, double value) throws ArrayIndexOutOfBoundsException;
	
	public void setObject(int index, Object o) throws ArrayIndexOutOfBoundsException, NullPointerException, ClassCastException;
	
}
