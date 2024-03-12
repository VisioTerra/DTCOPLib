package fr.visioterra.lib.image.dataBuffer;

/**
 * DataBufferCUSTOM class.
 * DataBuffer implementation for objects.
 * @author Grégory Mazabraud
 * 
 * <br>Changes :
 * <br>- 2013/11/20		|	Grégory Mazabraud		|	initial version
 */
public class DataBufferCUSTOM implements DataBuffer, java.io.Serializable {

	private static final long serialVersionUID = -7549019219169229128L;
	
	private final Object[] array;
	
	public DataBufferCUSTOM(int size) {
		this.array = new Object[size];
	}
	
	public DataBufferCUSTOM(Object[] array) {
		this.array = array;
	}
	
	@Override public int size() {
		return this.array.length;
	}
	
	@Override public int getDataType() {
		return DataType.TYPE_CUSTOM;
	}
	
	@Override public int getInt(int index) throws ArrayIndexOutOfBoundsException {
		throw new UnsupportedOperationException();
	}
	
	@Override public long getLong(int index) throws ArrayIndexOutOfBoundsException {
		throw new UnsupportedOperationException();
	}
	
	@Override public float getFloat(int index) throws ArrayIndexOutOfBoundsException {
		throw new UnsupportedOperationException();
	}
	
	@Override public double getDouble(int index) throws ArrayIndexOutOfBoundsException {
		throw new UnsupportedOperationException();
	}
	
	@Override public Object getObject(int index) throws ArrayIndexOutOfBoundsException {
		return this.array[index];
	}
	
	@Override public void setInt(int index, int value) throws ArrayIndexOutOfBoundsException {
		throw new UnsupportedOperationException();
	}
	
	@Override public void setLong(int index, long value) throws ArrayIndexOutOfBoundsException {
		throw new UnsupportedOperationException();
	}
	
	@Override public void setFloat(int index, float value) throws ArrayIndexOutOfBoundsException {
		throw new UnsupportedOperationException();
	}
	
	@Override public void setDouble(int index, double value) throws ArrayIndexOutOfBoundsException {
		throw new UnsupportedOperationException();
	}

	@Override public void setObject(int index, Object o) throws ArrayIndexOutOfBoundsException {
		this.array[index] = o;
	}
	
}
