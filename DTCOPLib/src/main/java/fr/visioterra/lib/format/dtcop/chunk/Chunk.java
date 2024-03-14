package fr.visioterra.lib.format.dtcop.chunk;

import java.util.HashMap;

import ucar.ma2.Array;
import ucar.ma2.DataType;
import ucar.ma2.Index;

public class Chunk {

	
	private static final double sqrt2 = Math.sqrt(2);
	private static final HashMap<Integer,float[]> coefsMap = new HashMap<>();
	
	private final Array array;
	private final int[] shape;
	
	
	static {
		
		int[] sizes = new int[] {4,8,16,32};	//{2,4,8,16,32,64,128,256,512};
		
		for(int size : sizes) {

			double pn = Math.PI / size;
			double sqrtSize = Math.sqrt(size);
			
			float[] coefs = new float[size * size];
			for (int k = 0; k < size; k++) {
				for(int n = 0 ; n < size ; n++) {
					coefs[n * size + k] = (float)(Math.cos(pn * (n + 0.5) * k) * (k == 0 ? 1.0 / sqrtSize : sqrt2 / sqrtSize)) ;
				}
			}
			coefsMap.put(size,coefs);
			
		}
		
	}

	
	private static void print2d(Array array, int[] shape, boolean round) {
		
		System.out.println("------------------------------");
		
		Index index = array.getIndex();
		
		for (int k1 = 0; k1 < shape[0]; k1++) {
			index.setDim(0, k1);
			
			for (int k2 = 0; k2 < shape[1]; k2++) {
				index.setDim(1, k2);
				
					
				if(round) {
					System.out.print(String.format("%7d"    ,array.getInt(index)));
				}
				else {
					System.out.print(String.format("%10.3f ",array.getDouble(index)));
				}
					
			}
			System.out.println();
		}
		System.out.println("------------------------------");
		
			
	}
	
	private static void print3d(Array array, int[] shape, boolean round) {
		
		System.out.println("------------------------------");
		
		Index index = array.getIndex();
		
		for (int k1 = 0; k1 < shape[0]; k1++) {
			index.setDim(0, k1);
			
			for (int k2 = 0; k2 < shape[1]; k2++) {
				index.setDim(1, k2);
				
				for (int k3 = 0; k3 < shape[2]; k3++) {
					index.setDim(2, k3);
					
					if(round) {
						System.out.print(String.format("%7d"    ,array.getInt(index)));
					}
					else {
						System.out.print(String.format("%10.3f ",array.getDouble(index)));
					}
					
				}
				System.out.println();
			}
			System.out.println("------------------------------");
		}
		System.out.println();
			
	}
	
	private static void dct1d(Array fArray, int[] origin, int dim, int size, float[] coefs) {

		Index idx = fArray.getIndex().set(origin);
		float[] tmp = new float[size];
		
		//copy value from fArray to tmp
		for (int k = 0; k < size; k++) {
			idx.setDim(dim, origin[dim] + k);
			tmp[k] = fArray.getFloat(idx);
		}

		//perform DCT from tmp values to fArray
		for (int k = 0; k < size; k++) {
			
			float sum = 0.0f;
			for(int n = 0 ; n < size ; n++) {
				sum += tmp[n] * coefs[n * size + k];
			}
			
			idx.setDim(dim, origin[dim] + k);
			fArray.setFloat(idx, sum);
		}
		
	}
	
	private static void idct1d(Array fArray, int[] origin, int dim, int size, float[] coefs) {
		
		Index idx = fArray.getIndex().set(origin);
		float[] tmp = new float[size];
		
		//copy value from fArray to tmp
		for (int n = 0; n < size; n++) {
			idx.setDim(dim, origin[dim] + n);
			tmp[n] = fArray.getFloat(idx);
		}
		
		//perform iDCT from tmp values to fArray
		for (int n = 0; n < size; n++) {

			float sum = 0.0f;
			for (int k = 0; k < size; k++) {
				sum += tmp[k] * coefs[n * size + k];
			}

			idx.setDim(dim, origin[dim] + n);
			fArray.setFloat(idx, sum);
	   }
	    
	}
	
	private static void dct2d(Array fArray, int[] chunkOrigin, int[] shape) {
		
		int size = shape[0];
		if(size != shape[1]) {
			throw new IllegalArgumentException();
		}
		
		float[] coefs = coefsMap.get(size);
		int[] origin = new int[2];
		
		origin[1] = chunkOrigin[1];
		for(int j = 0; j < size; j++) {
			origin[0] = chunkOrigin[0] + j;
			dct1d(fArray, origin, 1, size, coefs);
		}
		
		origin[0] = chunkOrigin[0];
		for (int i = 0; i < size; i++) {
			origin[1] = chunkOrigin[1] + i;
			dct1d(fArray, origin, 0, size, coefs);
		}

	}

	private static void idct2d(Array fArray, int[] chunkOrigin, int[] shape) {
		
		int size = shape[0];
		if(size != shape[1]) {
			throw new IllegalArgumentException();
		}
		
		float[] coefs = coefsMap.get(size);
		int[] origin = new int[3];
		
		origin[1] = chunkOrigin[1];
		for(int j = 0; j < size; j++) {
			origin[0] = chunkOrigin[0] + j;
			idct1d(fArray, origin, 1, size, coefs);
		}
		
		origin[0] = chunkOrigin[0];
		for (int i = 0; i < size; i++) {
			origin[1] = chunkOrigin[1] + i;
			idct1d(fArray, origin, 0, size, coefs);
		}
		
	}
	
	private static void dct3d(Array fArray, int[] chunkOrigin, int[] shape) {
		
		int size = shape[0];
		if(size != shape[1] || size != shape[2]) {
			throw new IllegalArgumentException();
		}
		
		float[] coefs = coefsMap.get(size);
		int[] origin = new int[3]; // {0,0,0};
		
		origin[2] = chunkOrigin[2];
		for(int k = 0; k < size; k++) {
			origin[0] = chunkOrigin[0] + k;
			for (int j = 0; j < size; j++) {
				origin[1] = chunkOrigin[1] + j;
				dct1d(fArray, origin, 2, size, coefs);
			}
		}

		origin[1] = chunkOrigin[1];
		for(int k = 0; k < size; k++) {
			origin[0] = chunkOrigin[0] + k;
			for (int i = 0; i < size; i++) {
				origin[2] = chunkOrigin[2] + i;
				dct1d(fArray, origin, 1, size, coefs);
			}
		}

		origin[0] = chunkOrigin[0];
		for(int j = 0; j < size; j++) {
			origin[1] = chunkOrigin[1] + j;
			for (int i = 0; i < size; i++) {
				origin[2] = chunkOrigin[2] + i;
				dct1d(fArray, origin, 0, size, coefs);
			}
		}

	}

	private static void idct3d(Array fArray, int[] chunkOrigin, int[] shape) {
		
		int size = shape[0];
		if(size != shape[1] || size != shape[2]) {
			throw new IllegalArgumentException();
		}
		
		float[] coefs = coefsMap.get(size);
		int[] origin = new int[3];
		
		origin[2] = chunkOrigin[2];
		for(int k = 0; k < size; k++) {
			origin[0] = chunkOrigin[0] + k;
			for (int j = 0; j < size; j++) {
				origin[1] = chunkOrigin[1] + j;
				idct1d(fArray, origin, 2, size, coefs);
			}
		}

		origin[1] = chunkOrigin[1];
		for(int k = 0; k < size; k++) {
			origin[0] = chunkOrigin[0] + k;
			for (int i = 0; i < size; i++) {
				origin[2] = chunkOrigin[2] + i;
				idct1d(fArray, origin, 1, size, coefs);
			}
		}

		origin[0] = chunkOrigin[0];
		for(int j = 0; j < size; j++) {
			origin[1] = chunkOrigin[1] + j;
			for (int i = 0; i < size; i++) {
				origin[2] = chunkOrigin[2] + i;
				idct1d(fArray, origin, 0, size, coefs);
			}
		}
		
	}
	
	private static void round(Array array, Index index, int dim) {
		
		int len = index.getShape(dim);
		
		if(dim == array.getRank() - 1) {
			for(int i = 0 ; i < len ; i++) {
				index.setDim(dim,i);
				float value = array.getFloat(index);
				array.setInt(index,Math.round(value));
			}
		}
		else {
			for(int i = 0 ; i < len ; i++) {
				index.setDim(dim,i);
				round(array,index,dim+1);
			}
		}
		
	}
	
	private static void scale(Array array, Index index, int dim, double scale, double offset) {
		
		int len = index.getShape(dim);
		
		if(dim == array.getRank() - 1) {
			for(int i = 0 ; i < len ; i++) {
				index.setDim(dim,i);
				double value = array.getDouble(index) * scale + offset;
				array.setDouble(index,value);
			}
		}
		else {
			for(int i = 0 ; i < len ; i++) {
				index.setDim(dim,i);
				scale(array,index,dim+1,scale,offset);
			}
		}
		
	}
	
	private static void diff(Array a1, Array a2, Index idx, int dim) {
		
		int len = idx.getShape(dim);
		
		if(dim == a1.getRank() - 1) {
			for(int i = 0 ; i < len ; i++) {
				idx.setDim(dim,i);
				a1.setFloat(idx, a1.getFloat(idx) - a2.getFloat(idx));
			}
		}
		else {
			for(int i = 0 ; i < len ; i++) {
				idx.setDim(dim,i);
				diff(a1,a2,idx,dim+1);
			}
		}
		
	}
	
	
	public Chunk(Array array) {
		this.array = array;
		this.shape = array.getShape();
	}
	
	public Chunk(DataType dataType, int[] shape, int[] values, Cell[] order) {
		
		this.array = Array.factory(dataType, shape);
		this.shape = shape;
		
		Index index = array.getIndex();
		
		for(int i = 0 ; i < values.length ; i++) {
			index.set(order[i].getCoordinates());
			array.setInt(index,values[i]);
		}
		
	}
	
	
	
	public int[] getShape() {
		return this.shape;
	}
	
	public DataType getDataType() {
		return this.array.getDataType();
	}
	

	
	//2D / 3D
	public void print(boolean round) {
		
		int[] shape = this.array.getShape();

		if(shape.length == 2) {
			print2d(this.array, shape, round);
		}
		else if(shape.length == 3) {
			print3d(this.array, shape, round);
		}
		else {
			throw new UnsupportedOperationException();
		}

	}
	
	//2D / 3D
	public void dct() {

		int[] shape = this.array.getShape();
		
		if(shape.length == 3) {
			dct3d(this.array, new int[] {0,0,0}, shape);
		}
		else if(shape.length == 2) {
			dct2d(this.array, new int[] {0,0}, shape);
		}
		else {
			throw new UnsupportedOperationException();
		}

	}

	//2D / 3D
	public void idct() {
		
		int[] shape = this.array.getShape();
		
		if(shape.length == 3) {
			idct3d(this.array, new int[] {0,0,0}, shape);
		}
		else if(shape.length == 2) {
			idct2d(this.array, new int[] {0,0}, shape);
		}
		else {
			throw new UnsupportedOperationException();
		}

	}
	
	
	//ND
	public Chunk copy() {
		return new Chunk(this.array.copy());
	}
	
	//ND
	public void round() {
		Index index = this.array.getIndex();
		round(this.array, index, 0);
	}
	
	//ND
	public void scale(double scale, double offset) {
		Index index = array.getIndex();
		scale(this.array, index, 0, scale, offset);
	}
	
	//3D
	public void scale(Chunk chunk, boolean mult, boolean round) {
		
		int[] shape = this.array.getShape();
		
		if(shape.length != 3) {
			throw new IllegalArgumentException();
		}
		
		Index index = this.array.getIndex();
		
		for (int k1 = 0; k1 < shape[0]; k1++) {
			index.setDim(0, k1);
			
			for (int k2 = 0; k2 < shape[1]; k2++) {
				index.setDim(1, k2);
				
				for (int k3 = 0; k3 < shape[2]; k3++) {
					index.setDim(2, k3);
					
					double value = this.array.getDouble(index);
					
					if(mult) {
						value = value * chunk.array.getDouble(index);
					}
					else {
						value = value / chunk.array.getDouble(index);
					}
					
					if(round) {
						this.array.setInt(index,(int)Math.round(value));
					}
					else {
						this.array.setDouble(index,value);
					}
					
				}
			}
		}
		
	}
	
	//3D
	public void roundTripScale(Chunk chunk) {
		
		int[] shape = this.array.getShape();
		
		if(shape.length != 3) {
			throw new IllegalArgumentException();
		}
		
		Index index = this.array.getIndex();
		
		for (int k1 = 0; k1 < shape[0]; k1++) {
			index.setDim(0, k1);
			
			for (int k2 = 0; k2 < shape[1]; k2++) {
				index.setDim(1, k2);
				
				for (int k3 = 0; k3 < shape[2]; k3++) {
					index.setDim(2, k3);
					
					double scale = chunk.array.getDouble(index);
					double value = this.array.getDouble(index) / scale;
					this.array.setDouble(index,Math.round(value) * scale);
					
				}
			}
		}
		
	}
	
	//ND
	public void diff(Chunk chunk) {
		
		int[] shape1 = this.array.getShape();
		int[] shape2 = chunk.array.getShape();
		
		if(shape1.length != shape2.length) {
			throw new IllegalArgumentException();
		}
		
		for(int i = 0 ; i < shape1.length ; i++) {
			if(shape1[i] != shape2[i]) {
				throw new IllegalArgumentException();	
			}
		}
		
		diff(this.array,chunk.array,this.array.getIndex(),0);
	}

	//3D
	public double getMaxAbsDiff(Chunk chunk) {
		
		int[] shape = this.array.getShape();
		Index index = this.array.getIndex();
		
		double diff = 0.0;
		
		for(int k = 0 ; k < shape[0] ; k++) {
			index.setDim(0,k);
			for(int j = 0 ; j < shape[1] ; j++) {
				index.setDim(1,j);
				for(int i = 0 ; i < shape[2] ; i++) {
					index.setDim(2,i);
					double d = Math.abs(this.array.getFloat(index) - chunk.array.getFloat(index));
					if(d > diff) {
						diff = d;
					}
				}
			}
		}
		
		return diff;
	}
	
	
	//ND
	public float[] getAsFloatArray(Cell[] order) {
		
//		if(cells == null || cells.length == 0) {
//			cells = Cell.order(this.array.getShape(),true);
//		}

		int idx = 0;
		float[] values = new float[order.length];
		
		Index index = this.array.getIndex();
		
		for(Cell cell : order) {
			index.set(cell.getCoordinates());
			values[idx++] = this.array.getFloat(index);
		}
		
		return values;
	}
	
	//ND
	public int[] getAsIntArray(Cell[] order) {
		
//		if(cells == null || cells.length == 0) {
//			cells = Cell.order(this.array.getShape(),true);
//		}

		int idx = 0;
		int[] values = new int[order.length];
		
		Index index = this.array.getIndex();
		
		for(Cell cell : order) {
			index.set(cell.getCoordinates());
//			values[idx++] = (int)Math.round(this.array.getFloat(index));
			values[idx++] = Math.round(this.array.getFloat(index));
		}
		
		return values;
	}
	
	
	
	private static void scaleRound(Array input, Array output, Index index, int dim, double scale, double offset) {
		
		int len = index.getShape(dim);
		
		if(dim == input.getRank() - 1) {
			for(int i = 0 ; i < len ; i++) {
				index.setDim(dim,i);
				double value = input.getDouble(index) * scale + offset;
				output.setInt(index,(int)Math.round(value));
			}
		}
		else {
			for(int i = 0 ; i < len ; i++) {
				index.setDim(dim,i);
				scaleRound(input,output,index,dim+1,scale,offset);
			}
		}
		
	}
	
	public static Chunk scaleRound(Chunk chunk, DataType outputDataType, double scale, double offset) {
		Array output = Array.factory(outputDataType,chunk.getShape());
		Index index = chunk.array.getIndex();
		scaleRound(chunk.array, output, index, 0, scale, offset);
		return new Chunk(output);
	}
	
}

