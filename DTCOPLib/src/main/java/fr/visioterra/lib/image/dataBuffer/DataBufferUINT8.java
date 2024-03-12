package fr.visioterra.lib.image.dataBuffer;

/**
 * DataBufferUINT8 class.
 * DataBuffer implementation for 8 bits unsigned integer values.
 * @author Gr�gory Mazabraud
 * 
 * <br>Changes :
 * <br>- 2013/11/20		|	Gr�gory Mazabraud		|	initial version
 */
public class DataBufferUINT8 implements DataBuffer, java.io.Serializable {

	private static final long serialVersionUID = -4743068126997102159L;
	
	private final byte[] array;
	
	public DataBufferUINT8(int size) {
		this.array = new byte[size];
	}
	
	public DataBufferUINT8(byte[] array) {
		this.array = array;
	}
	
	@Override public int size() {
		return this.array.length;
	}
	
	@Override public int getDataType() {
		return DataType.TYPE_UINT8;
	}
	
	@Override public int getInt(int index) throws ArrayIndexOutOfBoundsException {
		return this.array[index] & 0x000000ff;
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
	
	@Override public Integer getObject(int index) throws ArrayIndexOutOfBoundsException {
		return (Integer)getInt(index);
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
