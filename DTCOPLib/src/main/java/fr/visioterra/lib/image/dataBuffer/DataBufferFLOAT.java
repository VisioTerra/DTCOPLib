package fr.visioterra.lib.image.dataBuffer;

/**
 * DataBufferFLOAT class.
 * DataBuffer implementation for float values.
 * @author Grégory Mazabraud
 * 
 * <br>Changes :
 * <br>- 2013/11/20		|	Grégory Mazabraud		|	initial version
 */
public class DataBufferFLOAT implements DataBuffer, java.io.Serializable {

	private static final long serialVersionUID = -4064579334181449653L;
	
	private final float[] array;
	
	public DataBufferFLOAT(int size) {
		this.array = new float[size];
	}
	
	public DataBufferFLOAT(float[] array) {
		this.array = array;
	}
	
	@Override public int size() {
		return this.array.length;
	}
	
	@Override public int getDataType() {
		return DataType.TYPE_FLOAT;
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
	
	@Override public Float getObject(int index) throws ArrayIndexOutOfBoundsException {
		return (Float)this.array[index];
	}
	
	@Override public void setInt(int index, int value) throws ArrayIndexOutOfBoundsException {
		this.array[index] = (float)value;
	}
	
	@Override public void setLong(int index, long value) throws ArrayIndexOutOfBoundsException {
		this.array[index] = (float)value;
	}
	
	@Override public void setFloat(int index, float value) throws ArrayIndexOutOfBoundsException {
		this.array[index] = (float)value;
	}
	
	@Override public void setDouble(int index, double value) throws ArrayIndexOutOfBoundsException {
		this.array[index] = (float)value;
	}
	
	@Override public void setObject(int index, Object o) throws ArrayIndexOutOfBoundsException, NullPointerException, ClassCastException {
		this.array[index] = ((Number)o).floatValue();
	}
	
}
