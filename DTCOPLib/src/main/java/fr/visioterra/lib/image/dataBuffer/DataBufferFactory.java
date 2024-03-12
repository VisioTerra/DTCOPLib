package fr.visioterra.lib.image.dataBuffer;

/**
 * DataBufferFactory class.
 * Static method to instantiate DataBuffer from a data type tag
 * @author Grégory Mazabraud
 * 
 * <br>Changes :
 * <br>- 2013/11/20		|	Grégory Mazabraud		|	initial version
 * <br>- 2013/10/10		|	Grégory Mazabraud		|	add "raw copy" interface/class/method
 */
public class DataBufferFactory {
	
	public static DataBuffer getDataBuffer(int dataType, int size) throws IllegalArgumentException {
		
		switch(dataType) {
		
			case(DataType.TYPE_BINARY) : {
				return new DataBufferBINARY(size);
			}
			
			case(DataType.TYPE_INT8) : {
				return new DataBufferINT8(size);
			}
			
			case(DataType.TYPE_UINT8) : {
				return new DataBufferUINT8(size);
			}
			
			case(DataType.TYPE_INT16) : {
				return new DataBufferINT16(size);
			}
			
			case(DataType.TYPE_UINT16) : {
				return new DataBufferUINT16(size);
			}
			
			case(DataType.TYPE_INT32) : {
				return new DataBufferINT32(size);
			}
			
			case(DataType.TYPE_INT64) : {
				return new DataBufferINT64(size);
			}
			
			case(DataType.TYPE_FLOAT) : {
				return new DataBufferFLOAT(size);
			}
			
			case(DataType.TYPE_DOUBLE) : {
				return new DataBufferDOUBLE(size);
			}
			
			case(DataType.TYPE_CUSTOM) : {
				return new DataBufferCUSTOM(size);
			}
			
			default : {
				throw new IllegalArgumentException("Unsupported data type");
			}
			
		}
		
	}
	
	public static abstract class DataBufferRawCopy {
		
		protected final DataBuffer src;
		protected final DataBuffer dst;
		
		public DataBufferRawCopy(DataBuffer src, DataBuffer dst) {
			
			if(this.support(src.getDataType()) == false)
				throw new IllegalArgumentException("Data type " + DataType.getName(src.getDataType()) + " is not supported");
			
			if(this.support(dst.getDataType()) == false)
				throw new IllegalArgumentException("Data type " + DataType.getName(dst.getDataType()) + " is not supported");
			
			this.src = src;
			this.dst = dst;
		}
		
		public DataBuffer getSource() {
			return this.src;
		}
		
		public DataBuffer getDestination() {
			return this.dst;
		}
		
		public abstract boolean support(int dataType);
		
		public abstract void copy(int srcIndex, int dstIndex);
		
	}
	
	public static class DataBufferRawCopyInt extends DataBufferRawCopy {

		public DataBufferRawCopyInt(DataBuffer src, DataBuffer dst) {
			super(src, dst);
		}

		@Override public boolean support(int dataType) {
			return DataType.isIntType(dataType);
		}
		
		@Override public void copy(int srcIndex, int dstIndex) {
			this.dst.setInt(dstIndex, this.src.getInt(srcIndex));
		}
		
	}
	
	public static class DataBufferRawCopyLong extends DataBufferRawCopy {

		public DataBufferRawCopyLong(DataBuffer src, DataBuffer dst) {
			super(src, dst);
		}
		
		@Override public boolean support(int dataType) {
			return DataType.isLongType(dataType);
		}
		
		@Override public void copy(int srcIndex, int dstIndex) {
			this.dst.setLong(dstIndex, this.src.getLong(srcIndex));
		}
		
	}
	
	public static class DataBufferRawCopyFloat extends DataBufferRawCopy {

		public DataBufferRawCopyFloat(DataBuffer src, DataBuffer dst) {
			super(src, dst);
		}
		
		@Override public boolean support(int dataType) {
			return DataType.isFloatType(dataType);
		}
		
		@Override public void copy(int srcIndex, int dstIndex) {
			this.dst.setFloat(dstIndex, this.src.getFloat(srcIndex));
		}
		
	}
	
	public static class DataBufferRawCopyDouble extends DataBufferRawCopy {
		
		public DataBufferRawCopyDouble(DataBuffer src, DataBuffer dst) {
			super(src, dst);
		}
		
		@Override public boolean support(int dataType) {
			return DataType.isDoubleType(dataType);
		}
		
		@Override public void copy(int srcIndex, int dstIndex) {
			this.dst.setDouble(dstIndex, this.src.getDouble(srcIndex));
		}
		
	}
	
	public static class DataBufferRawCopyObject extends DataBufferRawCopy {
		
		public DataBufferRawCopyObject(DataBuffer src, DataBuffer dst) {
			super(src, dst);
		}
		
		@Override public boolean support(int dataType) {
			return DataType.isCustomType(dataType);
		}
		
		@Override public void copy(int srcIndex, int dstIndex) {
			this.dst.setObject(dstIndex, this.src.getObject(srcIndex));
		}
		
	}
	
	public static DataBufferRawCopy rawCopy(DataBuffer src, DataBuffer dst) throws IllegalArgumentException {

		if(src.getDataType() != dst.getDataType())
			throw new IllegalArgumentException("incompatible data type");

		if(DataType.isIntType(src.getDataType())) {
			return new DataBufferRawCopyInt(src, dst);
		}
		
		else if(DataType.isLongType(src.getDataType())) {
			return new DataBufferRawCopyLong(src, dst);
		}
		
		else if(DataType.isFloatType(src.getDataType())) {
			return new DataBufferRawCopyFloat(src, dst);
		}
		
		else if(DataType.isDoubleType(src.getDataType())) {
			return new DataBufferRawCopyDouble(src, dst);
		}
		
		else if(DataType.isCustomType(src.getDataType())) {
			return new DataBufferRawCopyObject(src, dst);
		}
		
		throw new IllegalArgumentException("Unsupported data type");
	}
	
	public static DataBufferRawCopy rawCopy(DataBuffer src, int dstSize) {
		return rawCopy(src,getDataBuffer(src.getDataType(),dstSize));
	}
	
}
