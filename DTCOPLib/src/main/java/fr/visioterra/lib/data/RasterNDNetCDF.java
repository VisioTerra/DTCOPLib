package fr.visioterra.lib.data;


import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import fr.visioterra.lib.image.dataBuffer.DataType;
import ucar.ma2.Array;
import ucar.ma2.Index;
import ucar.ma2.Section;
import ucar.nc2.Attribute;
import ucar.nc2.Dimension;
import ucar.nc2.NetcdfFile;
import ucar.nc2.NetcdfFiles;
import ucar.nc2.Variable;

public class RasterNDNetCDF implements RasterND {

	
	private static final String[] scaleFactorKeys = new String[] { "scale_factor" };
	private static final String[] addOffsetKeys   = new String[] { "add_offset" };
//	private static final String[] fillValueKeys   = new String[] { "_FillValue", "missing_value" };
//	private static final String[] unitsKeys       = new String[] { "units" };
//	private static final String[] longNameKeys    = new String[] { "long_name" };
	
//	"standard_name";
//	"valid_max";
//	"valid_min";
//	"_ChunkSizes";
	
//	public final Benchmark bench1 = new Benchmark("getArray");
	
	public static boolean debug = false;
	private final NetcdfFile ncfile;
	
	private final Variable dataVar;
	private final int dataType;
	private final double dataScaleFactor;
	private final double dataAddOffset;
	
	

	private static double getAttributeAsDouble(Variable variable, String[] attributeNames, double defaultValue) {
		
		if(variable == null) {
			return defaultValue;
		}
		
		for(String attributeName : attributeNames) {
			
			Attribute attr = variable.findAttribute(attributeName);
			
			//attr not null
			if(attr != null) {
				
				//attr is String
				if(attr.getDataType().isString()) {
					
					String s = attr.getStringValue();
					if(s.equalsIgnoreCase("nan")) {
						return Double.NaN;
					}
					else {
						try {
							return Double.parseDouble(s);
						}
						catch (NumberFormatException e) {
						}
					}	
				}
				
				//attr is not String
				else {
					Number number = attr.getNumericValue();
					if(number != null) {
						return number.doubleValue();
					}
				}
	
			}
			
		}
		
		return defaultValue;
	}
	
	private static int getDataType(ucar.ma2.DataType type) throws IllegalArgumentException {

		switch (type) {

			case BOOLEAN: {
				return DataType.TYPE_BINARY;
			}

			case BYTE: {
				return DataType.TYPE_INT8;
			}

			case CHAR: {
				return DataType.TYPE_UINT8;
			}
			
			case UBYTE: {
				return DataType.TYPE_UINT8;
			}

			case USHORT: {
				return DataType.TYPE_UINT16;
			}
			
			case SHORT: {
				return DataType.TYPE_INT16;
			}
			
			case UINT: {
				return DataType.TYPE_INT32;
			}

			case INT: {
				return DataType.TYPE_INT32;
			}

			case LONG: {
				return DataType.TYPE_INT64;
			}

			case FLOAT: {
				return DataType.TYPE_FLOAT;
			}

			case DOUBLE: {
				return DataType.TYPE_DOUBLE;
			}

			default: {
				throw new IllegalArgumentException("Unsupported dataBufferType : " + type);
			}
		}
	}

	
	public RasterNDNetCDF(File inputFile, String datVarName) throws Exception {
		
		Objects.requireNonNull(datVarName);
		
		if(false == NetcdfFiles.canOpen(inputFile.getAbsolutePath())) {
			throw new Exception("Cannot open file " + inputFile.getAbsolutePath() + " (Unrecognize as NetCDF file)");
		}
		
		this.ncfile = NetcdfFiles.open(inputFile.getAbsolutePath());
		this.dataVar = this.ncfile.findVariable(datVarName);
		
		if(this.dataVar == null) {
			try {
				this.ncfile.close();
			} catch(Exception e) { }
			throw new IllegalArgumentException("Invalid var name (" + datVarName + ")");
		}
		
		this.dataType        = getDataType(this.dataVar.getDataType());
		this.dataScaleFactor = getAttributeAsDouble(this.dataVar, scaleFactorKeys, Double.NaN);
		this.dataAddOffset   = getAttributeAsDouble(this.dataVar, addOffsetKeys, Double.NaN);
		
	}
	
	public RasterNDNetCDF(File inputFile, String datVarName, int dataType) throws Exception {
		
		Objects.requireNonNull(datVarName);
		
		if(false == NetcdfFiles.canOpen(inputFile.getAbsolutePath())) {
			throw new Exception("Cannot open file " + inputFile.getAbsolutePath() + " (Unrecognize as NetCDF file)");
		}
		
		this.ncfile = NetcdfFiles.open(inputFile.getAbsolutePath());
		this.dataVar = this.ncfile.findVariable(datVarName);
		
		if(this.dataVar == null) {
			try {
				this.ncfile.close();
			} catch(Exception e) { }
			throw new IllegalArgumentException("Invalid var name (" + datVarName + ")");
		}
		
		this.dataType        = dataType;
		this.dataScaleFactor = getAttributeAsDouble(this.dataVar, scaleFactorKeys, Double.NaN);
		this.dataAddOffset   = getAttributeAsDouble(this.dataVar, addOffsetKeys, Double.NaN);
		
	}
	

	public Variable getVariable() {
		return this.dataVar;
	}
	
	public Collection<Variable> getVariables() {
		return ncfile.getVariables();
//		return variableMap.values();
	}
	
	/*
	public Raster2D getSamples(int i, int j, int w, int h, int b) throws Exception {
		
		Array array = getArray(i,j,w,h,b);
		
		ByteBuffer dataAsByteBuffer = array.getDataAsByteBuffer();
		byte[] bands = dataAsByteBuffer.array();
		
		return new Raster2DByteBufferBSQ(w, h, 1, this.type, bands, dataAsByteBuffer.order());
	}
	
	public Raster2D getSamples(int i, int j, int k, int w, int h, int d) throws Exception {
		
		Section section = new Section(new Range(k,k+d-1), new Range(j,j+h-1), new Range(i,i+w-1));
		Array array = getArray(section);
		
		ByteBuffer dataAsByteBuffer = array.getDataAsByteBuffer();
		byte[] bands = dataAsByteBuffer.array();
		
		return new Raster2DByteBufferBSQ(w, h, d, this.type, bands, dataAsByteBuffer.order());
	}
	*/	

	
	
	//RasterNDMetadata
	
	@Override public List<Dimension> getDimensions() {
		return this.dataVar.getDimensions();
	}

	@Override public int[] getShape() {
		return this.dataVar.getShape();
	}

	@Override public int getLength(int dimension) {
		return this.dataVar.getDimension(dimension).getLength();
	}

	@Override public int getLength(String dimension) {
		
		int index = this.dataVar.findDimensionIndex(dimension);
		
		if(index < 0) {
			return -1;
		}
		else {
			return getLength(index);
		}
	}

	@Override public int getDataType() {
		return this.dataType;
	}

	@Override public double getScaleFactor() {
		return this.dataScaleFactor;
	}
	
	@Override public double getAddOffset() {
		return this.dataAddOffset;
	}

	
	//RasterND
	@Override public ucar.ma2.DataType getArrayDataType() {
		return dataVar.getDataType();
	}
	
	@Override public Array getArray(Section section) throws Exception {
//		return this.dataVar.read(section);
		
//		bench1.start();
		Array array = this.dataVar.read(section);
//		bench1.stop();
		
		return array;
	}
	
	@Override public int getSampleInt(int[] coordinates) throws Exception {
		throw new UnsupportedOperationException("Cannot extract only one value");
	}

	@Override public long getSampleLong(int[] coordinates) throws Exception {
		throw new UnsupportedOperationException("Cannot extract only one value");
	}

	@Override public float getSampleFloat(int[] coordinates) throws Exception {
		throw new UnsupportedOperationException("Cannot extract only one value");
	}

	@Override public double getSampleDouble(int[] coordinates) throws Exception {
		throw new UnsupportedOperationException("Cannot extract only one value");
	}

	@Override public int getSampleInt(Index index) throws Exception {
		throw new UnsupportedOperationException("Cannot extract only one value");
	}

	@Override public long getSampleLong(Index index) throws Exception {
		throw new UnsupportedOperationException("Cannot extract only one value");
	}

	@Override public float getSampleFloat(Index index) throws Exception {
		throw new UnsupportedOperationException("Cannot extract only one value");
	}

	@Override public double getSampleDouble(Index index) throws Exception {
		throw new UnsupportedOperationException("Cannot extract only one value");
	}

	@Override public void close() throws IOException {
		this.ncfile.close();
	}

	
}
