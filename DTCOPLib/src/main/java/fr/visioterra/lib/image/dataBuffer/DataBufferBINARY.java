package fr.visioterra.lib.image.dataBuffer;

/**
 * DataBufferBINARY class.
 * DataBuffer implementation for binary values.
 * @author Grégory Mazabraud
 * 
 * <br>Changes :
 * <br>- 2013/11/20		|	Grégory Mazabraud		|	initial version
 */
public class DataBufferBINARY implements DataBuffer, java.io.Serializable {

	private static final long serialVersionUID = 3836587039024929105L;

	private final static int elementSize = Integer.SIZE;
	
	private final int size;
	private final int[] array; //int[] is theoretically faster than byte[]. In practice, no difference 
	
	
	private int getBit(int index) throws ArrayIndexOutOfBoundsException {
		if( (this.array[index / elementSize] & (1 << (index % elementSize))) == 0)
			return 0;
		else
			return 1;
	}
	
	private void setBit(int index, int value) throws ArrayIndexOutOfBoundsException {
		
		if(value == 0) {
			this.array[index / elementSize] &= ~(1 << (index % elementSize));	
		} else {
			this.array[index / elementSize] |=  (1 << (index % elementSize));
		}
		
	}
	
	public DataBufferBINARY(int size) {
		this.size = size;
		this.array = new int[(size % elementSize == 0) ? size / elementSize : size / elementSize + 1];
	}
	
	public DataBufferBINARY(int[] array, int size) {
		this.size = size;
		this.array = array;
	}
	
	@Override public int size() {
		return this.size;
	}
	
	@Override public int getDataType() {
		return DataType.TYPE_BINARY;
	}
	
	@Override public int getInt(int index) throws ArrayIndexOutOfBoundsException {
		return (int)getBit(index);
	}
	
	@Override public long getLong(int index) throws ArrayIndexOutOfBoundsException {
		return (long)getBit(index);
	}
	
	@Override public float getFloat(int index) throws ArrayIndexOutOfBoundsException {
		return (float)getBit(index);
	}
	
	@Override public double getDouble(int index) throws ArrayIndexOutOfBoundsException {
		return (double)getBit(index);
	}
	
	@Override public Boolean getObject(int index) throws ArrayIndexOutOfBoundsException {
		return getBit(index) != 0;
	}
	
	@Override public void setInt(int index, int value) throws ArrayIndexOutOfBoundsException {
		setBit(index,(int)value);
	}
	
	@Override public void setLong(int index, long value) throws ArrayIndexOutOfBoundsException {
		setBit(index,(int)value);
	}
	
	@Override public void setFloat(int index, float value) throws ArrayIndexOutOfBoundsException {
		setBit(index,(int)value);
	}
	
	@Override public void setDouble(int index, double value) throws ArrayIndexOutOfBoundsException {
		setBit(index,(int)value);
	}

	@Override public void setObject(int index, Object o) throws ArrayIndexOutOfBoundsException, NullPointerException, ClassCastException {
		if(((Boolean)o).booleanValue())
			setBit(index,1);
		else
			setBit(index,0);
	}
	
}
