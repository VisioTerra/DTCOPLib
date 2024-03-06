package fr.visioterra.lib.image.dataBuffer;


import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;

/**
 * DataType class.
 * @author Grégory Mazabraud
 * 
 * <br>Changes :
 * <br>- 2013/11/18		|	Grégory Mazabraud		|	initial version
 */
public class DataType {
	
	/*
		0baaaabbbbccccddddeeeeffffgggghhhh
		                  eeeeffffgggghhhh : type size, in bits
		          c                        : 1=numeric / 0=not numeric
		          1c  0000                 : 1=real / 0=not real
		          1000c                    : 1=integer / 0=not integer
		          10001c                   : 1=unsigned / 0=signed
		          10001xc                  : 1=int32 can hold this data type / 0=int32 cannot this hold data type
	*/
	
//	private static final int sizeMask = 0x0000ffff;
//	private static final int isNumericMask = 0x00800000;
//	private static final int is
	
	
	public static final int TYPE_BINARY =  1;
	public static final int TYPE_INT8   =  2;
	public static final int TYPE_UINT8  =  3;
	public static final int TYPE_INT16  =  4;
	public static final int TYPE_UINT16 =  5;
	public static final int TYPE_INT32  =  6;
	public static final int TYPE_INT64  =  7;
	public static final int TYPE_FLOAT  =  8;
	public static final int TYPE_DOUBLE =  9;
	public static final int TYPE_CUSTOM = 10;
	

	private static final HashMap<Integer,String>  dataTypeName;
	private static final HashMap<String,Integer>  dataTypeInverseName;
	private static final HashSet<Integer>         dataTypeNumeric;
	private static final HashMap<Integer,Integer> dataTypeSize;
	private static final HashMap<Integer,Integer> dataTypeMin;
	private static final HashMap<Integer,Integer> dataTypeMax;
	private static final HashSet<Integer>         dataTypeInt;
	private static final HashSet<Integer>         dataTypeReal;
	private static final HashSet<Integer>         dataTypeSigned;
	
	static {

		dataTypeName = new HashMap<Integer,String>();
		dataTypeName.put(TYPE_BINARY,"TYPE_BINARY");
		dataTypeName.put(TYPE_INT8  ,"TYPE_INT8"  );
		dataTypeName.put(TYPE_UINT8 ,"TYPE_UINT8" );
		dataTypeName.put(TYPE_INT16 ,"TYPE_INT16" );
		dataTypeName.put(TYPE_UINT16,"TYPE_UINT16");
		dataTypeName.put(TYPE_INT32 ,"TYPE_INT32" );
		dataTypeName.put(TYPE_INT64 ,"TYPE_INT64" );
		dataTypeName.put(TYPE_FLOAT ,"TYPE_FLOAT" );
		dataTypeName.put(TYPE_DOUBLE,"TYPE_DOUBLE");
		dataTypeName.put(TYPE_CUSTOM,"TYPE_CUSTOM");
		
		dataTypeInverseName = new HashMap<String,Integer>();
		for(Entry<Integer,String> entry : dataTypeName.entrySet()) {
			dataTypeInverseName.put(entry.getValue(), entry.getKey());
		}
		
		dataTypeNumeric = new HashSet<Integer>();
		dataTypeNumeric.add(TYPE_BINARY);
		dataTypeNumeric.add(TYPE_INT8  );
		dataTypeNumeric.add(TYPE_UINT8 );
		dataTypeNumeric.add(TYPE_INT16 );
		dataTypeNumeric.add(TYPE_UINT16);
		dataTypeNumeric.add(TYPE_INT32 );
		dataTypeNumeric.add(TYPE_INT64 );
		dataTypeNumeric.add(TYPE_FLOAT );
		dataTypeNumeric.add(TYPE_DOUBLE);
		
		dataTypeSize = new HashMap<Integer,Integer>();
		dataTypeSize.put(TYPE_BINARY,  1);
		dataTypeSize.put(TYPE_INT8  ,  8);
		dataTypeSize.put(TYPE_UINT8 ,  8);
		dataTypeSize.put(TYPE_INT16 , 16);
		dataTypeSize.put(TYPE_UINT16, 16);
		dataTypeSize.put(TYPE_INT32 , 32);
		dataTypeSize.put(TYPE_INT64 , 64);
		dataTypeSize.put(TYPE_FLOAT , 32);
		dataTypeSize.put(TYPE_DOUBLE, 64);
//		dataTypeSize.put(TYPE_CUSTOM, -1);
		
		dataTypeMin = new HashMap<Integer,Integer>();
		dataTypeMin.put(TYPE_BINARY,  0);
		dataTypeMin.put(TYPE_INT8  ,  -128);
		dataTypeMin.put(TYPE_UINT8 ,  0);
		dataTypeMin.put(TYPE_INT16 ,  -32768);
		dataTypeMin.put(TYPE_UINT16,  0);
		dataTypeMin.put(TYPE_INT32 ,  Integer.MIN_VALUE);
		
		dataTypeMax = new HashMap<Integer,Integer>();
		dataTypeMax.put(TYPE_BINARY,  1);
		dataTypeMax.put(TYPE_INT8  ,  127);
		dataTypeMax.put(TYPE_UINT8 ,  255);
		dataTypeMax.put(TYPE_INT16 ,  32767);
		dataTypeMax.put(TYPE_UINT16,  65535);
		dataTypeMax.put(TYPE_INT32 ,  Integer.MAX_VALUE);
		
		
		dataTypeInt = new HashSet<Integer>();
		dataTypeInt.add(TYPE_BINARY);
		dataTypeInt.add(TYPE_INT8);
		dataTypeInt.add(TYPE_UINT8);
		dataTypeInt.add(TYPE_INT16);
		dataTypeInt.add(TYPE_UINT16);
		dataTypeInt.add(TYPE_INT32);

		
		dataTypeReal = new HashSet<Integer>();
		dataTypeReal.add(TYPE_FLOAT);
		dataTypeReal.add(TYPE_DOUBLE);
		
		
		dataTypeSigned = new HashSet<Integer>();
		dataTypeSigned.add(TYPE_INT8);
		dataTypeSigned.add(TYPE_INT64);
		dataTypeSigned.add(TYPE_INT32);
		dataTypeSigned.add(TYPE_INT16);
		dataTypeSigned.add(TYPE_FLOAT);
		dataTypeSigned.add(TYPE_DOUBLE);
		
	}
	
	public static String getName(int dataType) throws IllegalArgumentException {
		String s = dataTypeName.get(dataType);
		if(s != null)
			return s;
		else
			throw new IllegalArgumentException("Invalid DataType");
	}
	
	public static int getDataType(String name) throws IllegalArgumentException {
		Integer i = dataTypeInverseName.get(name);
		if(i != null)
			return i;
		else
			throw new IllegalArgumentException("Invalid DataType name");
	}

	public static boolean isNumericType(int dataType) {
		return dataTypeNumeric.contains(dataType);
	}
	
	public static int getSize(int dataType) throws IllegalArgumentException {
		Integer i = dataTypeSize.get(dataType);
		if(i != null)
			return i;
		else
			throw new IllegalArgumentException("Invalid DataType");
	}
	
	public static int getMin(int dataType) throws IllegalArgumentException {
		Integer i = dataTypeMin.get(dataType);
		if(i != null)
			return i;
		else
			throw new IllegalArgumentException("Invalid DataType");
	}
	
	public static int getMax(int dataType) throws IllegalArgumentException {
		Integer i = dataTypeMax.get(dataType);
		if(i != null)
			return i;
		else
			throw new IllegalArgumentException("Invalid DataType");
	}
	
	public static boolean isSignedType(int dataType){
		return DataType.dataTypeSigned.contains(dataType);
	}
	
	public static boolean isIntType(int dataType) {
		return dataTypeInt.contains(dataType);
	}
	
	public static boolean isLongType(int dataType) {
		return dataType == TYPE_INT64;
	}
	
	public static boolean isRealType(int dataType) {
		return dataTypeReal.contains(dataType);
	}
	
	public static boolean isFloatType(int dataType) {
		return dataType == TYPE_FLOAT;
	}
	
	public static boolean isDoubleType(int dataType) {
		return dataType == TYPE_DOUBLE;
	}
	
	public static boolean isCustomType(int dataType) {
		return dataType == TYPE_CUSTOM;
	}
	
	public static int unsignedByteToInt(byte b) {
		return b & 0x000000ff;
	}
	
	public static int unsignedShortToInt(short s) {
		return s & 0x0000ffff;
	}
	
	public static long unsignedIntToLong(int i) {
		return i & 0x00000000ffffffffL;
//		return (i < 0) ? (long) i + 4294967296L : (long) i;
	}
	  
}




