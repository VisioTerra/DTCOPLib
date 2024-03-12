package fr.visioterra.lib.image.dataBuffer;

/**
 * DataBufferINT32 class.
 * DataBuffer implementation for 32 bits signed integer values.
 * @author Grégory Mazabraud
 * 
 * <br>Changes :
 * <br>- 2013/11/20		|	Grégory Mazabraud		|	initial version
 */
public class DataBufferINT32 implements DataBuffer, java.io.Serializable {

	private static final long serialVersionUID = 8067393590453696373L;
	
	private final int[] array;
	
	public DataBufferINT32(int size) {
		this.array = new int[size];
	}
	
	public DataBufferINT32(int[] array) {
		this.array = array;
	}
	
	@Override public int size() {
		return this.array.length;
	}
	
	@Override public int getDataType() {
		return DataType.TYPE_INT32;
	}
	
	@Override public int getInt(int index) throws ArrayIndexOutOfBoundsException {
		return (int)this.array[index];
	}
	
	@Override public long getLong(int index) throws ArrayIndexOutOfBoundsException {
		return (long)this.array[index];
	}
	
	@Override public float getFloat(int index) throws ArrayIndexOutOfBoundsException {
		return (float)this.array[index];
	}
	
	@Override public double getDouble(int index) throws ArrayIndexOutOfBoundsException {
		return (double)this.array[index];
	}
	
	@Override public Integer getObject(int index) throws ArrayIndexOutOfBoundsException {
		return (Integer)this.array[index];
	}
	
	@Override public void setInt(int index, int value) throws ArrayIndexOutOfBoundsException {
		this.array[index] = (int)value;
	}
	
	@Override public void setLong(int index, long value) throws ArrayIndexOutOfBoundsException {
		this.array[index] = (int)value;
	}
	
	@Override public void setFloat(int index, float value) throws ArrayIndexOutOfBoundsException {
		this.array[index] = (int)value;
	}
	
	@Override public void setDouble(int index, double value) throws ArrayIndexOutOfBoundsException {
		this.array[index] = (int)value;
	}
	
	@Override public void setObject(int index, Object o) throws ArrayIndexOutOfBoundsException, NullPointerException, ClassCastException {
		this.array[index] = ((Number)o).intValue();
	}
	
}
