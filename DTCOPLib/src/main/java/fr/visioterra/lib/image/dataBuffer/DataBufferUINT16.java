package fr.visioterra.lib.image.dataBuffer;

/**
 * DataBufferUINT16 class.
 * DataBuffer implementation for 16 bits unsigned integer values.
 * @author Gr�gory Mazabraud
 * 
 * <br>Changes :
 * <br>- 2013/11/20		|	Gr�gory Mazabraud		|	initial version
 */
public class DataBufferUINT16 implements DataBuffer, java.io.Serializable {

	private static final long serialVersionUID = 3108813670088883991L;
	
	private final short[] array;
	
	public DataBufferUINT16(int size) {
		this.array = new short[size];
	}
	
	public DataBufferUINT16(short[] array) {
		this.array = array;
	}
	
	@Override public int size() {
		return this.array.length;
	}
	
	@Override public int getDataType() {
		return DataType.TYPE_UINT16;
	}
	
	@Override public int getInt(int index) throws ArrayIndexOutOfBoundsException {
		return this.array[index] & 0x0000ffff;
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
		this.array[index] = (short)value;
	}
	
	@Override public void setLong(int index, long value) throws ArrayIndexOutOfBoundsException {
		this.array[index] = (short)value;
	}
	
	@Override public void setFloat(int index, float value) throws ArrayIndexOutOfBoundsException {
		this.array[index] = (short)value;
	}
	
	@Override public void setDouble(int index, double value) throws ArrayIndexOutOfBoundsException {
		this.array[index] = (short)value;
	}
	
	@Override public void setObject(int index, Object o) throws ArrayIndexOutOfBoundsException, NullPointerException, ClassCastException {
		this.array[index] = (short)((Number)o).intValue();
	}
	
}
