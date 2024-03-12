package fr.visioterra.lib.image.dataBuffer;

/**
 * DataBufferINT64 class.
 * DataBuffer implementation for 64 bits signed integer values.
 * @author Grégory Mazabraud
 * 
 * <br>Changes :
 * <br>- 2013/11/20		|	Grégory Mazabraud		|	initial version
 */
public class DataBufferINT64 implements DataBuffer, java.io.Serializable {

	private static final long serialVersionUID = 7067562325888845715L;
	
	private final long[] array;
	
	public DataBufferINT64(int size) {
		this.array = new long[size];
	}
	
	public DataBufferINT64(long[] array) {
		this.array = array;
	}
	
	@Override public int size() {
		return this.array.length;
	}
	
	@Override public int getDataType() {
		return DataType.TYPE_INT64;
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
	
	@Override public Long getObject(int index) throws ArrayIndexOutOfBoundsException {
		return (Long)this.array[index];
	}
	
	@Override public void setInt(int index, int value) throws ArrayIndexOutOfBoundsException {
		this.array[index] = (long)value;
	}
	
	@Override public void setLong(int index, long value) throws ArrayIndexOutOfBoundsException {
		this.array[index] = (long)value;
	}
	
	@Override public void setFloat(int index, float value) throws ArrayIndexOutOfBoundsException {
		this.array[index] = (long)value;
	}
	
	@Override public void setDouble(int index, double value) throws ArrayIndexOutOfBoundsException {
		this.array[index] = (long)value;
	}
	
	@Override public void setObject(int index, Object o) throws ArrayIndexOutOfBoundsException, NullPointerException, ClassCastException {
		this.array[index] = ((Number)o).longValue();
	}
}
