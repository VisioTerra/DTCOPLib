package fr.visioterra.lib.image.dataBuffer;

/**
 * DataBufferDOUBLE class.
 * DataBuffer implementation for double values.
 * @author Grégory Mazabraud
 * 
 * <br>Changes :
 * <br>- 2013/11/20		|	Grégory Mazabraud		|	initial version
 */
public class DataBufferDOUBLE implements DataBuffer, java.io.Serializable {

	private static final long serialVersionUID = 163787869314717752L;
	
	private final double[] array;
	
	public DataBufferDOUBLE(int size) {
		this.array = new double[size];
	}
	
	public DataBufferDOUBLE(double[] array) {
		this.array = array;
	}
	
	@Override public int size() {
		return this.array.length;
	}
	
	@Override public int getDataType() {
		return DataType.TYPE_DOUBLE;
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
	
	@Override public Double getObject(int index) throws ArrayIndexOutOfBoundsException {
		return (Double)this.array[index];
	}
	
	@Override public void setInt(int index, int value) throws ArrayIndexOutOfBoundsException {
		this.array[index] = (double)value;
	}
	
	@Override public void setLong(int index, long value) throws ArrayIndexOutOfBoundsException {
		this.array[index] = (double)value;
	}
	
	@Override public void setFloat(int index, float value) throws ArrayIndexOutOfBoundsException {
		this.array[index] = (double)value;
	}
	
	@Override public void setDouble(int index, double value) throws ArrayIndexOutOfBoundsException {
		this.array[index] = (double)value;
	}

	@Override public void setObject(int index, Object o) throws ArrayIndexOutOfBoundsException, NullPointerException, ClassCastException {
		this.array[index] = ((Number)o).doubleValue();
	}
	
}
