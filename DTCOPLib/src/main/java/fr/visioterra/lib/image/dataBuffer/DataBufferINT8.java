package fr.visioterra.lib.image.dataBuffer;

/**
 * DataBufferINT8 class.
 * DataBuffer implementation for 8 bits signed integer values.
 * @author Grégory Mazabraud
 * 
 * <br>Changes :
 * <br>- 2013/11/20		|	Grégory Mazabraud		|	initial version
 */
public class DataBufferINT8 implements DataBuffer, java.io.Serializable {

	private static final long serialVersionUID = 4744290542359621219L;
	
	private final byte[] array;
	
	public DataBufferINT8(int size) {
		this.array = new byte[size];
	}
	
	public DataBufferINT8(byte[] array) {
		this.array = array;
	}
	
	@Override public int size() {
		return this.array.length;
	}
	
	@Override public int getDataType() {
		return DataType.TYPE_INT8;
	}
	
	@Override public int getInt(int index) throws ArrayIndexOutOfBoundsException {
		return this.array[index];
	}
	
	@Override public long getLong(int index) throws ArrayIndexOutOfBoundsException {
		return (long)getInt(index);
	}
	
	@Override public float getFloat(int index) throws ArrayIndexOutOfBoundsException {
		return (float)getInt(index);
	}
	
	@Override public double getDouble(int index) throws ArrayIndexOutOfBoundsException {
		return (double)getInt(index);
	}
	
	@Override public Byte getObject(int index) throws ArrayIndexOutOfBoundsException {
		return (Byte)this.array[index];
	}
	
	@Override public void setInt(int index, int value) throws ArrayIndexOutOfBoundsException {
		this.array[index] = (byte)value;
	}
	
	@Override public void setLong(int index, long value) throws ArrayIndexOutOfBoundsException {
		this.array[index] = (byte)value;
	}
	
	@Override public void setFloat(int index, float value) throws ArrayIndexOutOfBoundsException {
		this.array[index] = (byte)value;
	}
	
	@Override public void setDouble(int index, double value) throws ArrayIndexOutOfBoundsException {
		this.array[index] = (byte)value;
	}
	
	@Override public void setObject(int index, Object o) throws ArrayIndexOutOfBoundsException, NullPointerException, ClassCastException {
		this.array[index] = (byte)((Number)o).intValue();
	}
	
}
