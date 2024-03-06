package fr.visioterra.lib.data;


import fr.visioterra.lib.image.dataBuffer.DataType;
import ucar.ma2.Array;
import ucar.ma2.Index;
import ucar.ma2.Section;

public interface RasterND extends RasterNDMetadata, AutoCloseable {
	
	
	public static ucar.ma2.DataType getArrayDataType(int dataType) {
		
		if(dataType == DataType.TYPE_INT8) {
			return ucar.ma2.DataType.BYTE;
		}
		else if(dataType == DataType.TYPE_UINT8) {
			return ucar.ma2.DataType.UBYTE;
		}
		else if(dataType == DataType.TYPE_INT16) {
			return ucar.ma2.DataType.SHORT;
		}
		else if(dataType == DataType.TYPE_UINT16) {
			return ucar.ma2.DataType.USHORT;
		}
		else if(dataType == DataType.TYPE_INT32) {
			return ucar.ma2.DataType.INT;
		}
		else if(dataType == DataType.TYPE_INT64) {
			return ucar.ma2.DataType.LONG;
		}
		else if(dataType == DataType.TYPE_FLOAT) {
			return ucar.ma2.DataType.FLOAT;
		}
		else if(dataType == DataType.TYPE_DOUBLE) {
			return ucar.ma2.DataType.DOUBLE;
		}
		
		return null;
	}

	ucar.ma2.DataType getArrayDataType();
	
	default public Array getArray(int[] origin, int[] shape) throws Exception {
		return getArray(new Section(origin, shape));
	}
	
	public Array getArray(Section section) throws Exception;
	
	public int getSampleInt(int[] coordinates) throws Exception;

	public long getSampleLong(int[] coordinates) throws Exception;

	public float getSampleFloat(int[] coordinates) throws Exception;

	public double getSampleDouble(int[] coordinates) throws Exception;
	
	public int getSampleInt(Index index) throws Exception;
	
	public long getSampleLong(Index index) throws Exception;
	
	public float getSampleFloat(Index index) throws Exception;
	
	public double getSampleDouble(Index index) throws Exception;
	
	
	@Override public void close() throws Exception;
}
