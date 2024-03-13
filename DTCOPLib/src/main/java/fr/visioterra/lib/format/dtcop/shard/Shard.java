package fr.visioterra.lib.format.dtcop.shard;

import fr.visioterra.lib.format.dtcop.chunk.Chunk;
import ucar.ma2.Array;
import ucar.ma2.DataType;
import ucar.ma2.Index;

public class Shard {
	
	private final Array array;
	private final int[] shape;
	private final int[] chunkShape;
	private final int[] numChunk;
//	private final double scaleFactor;
//	private final double addOffset;

	
	private static void copyInt(Array input,  Index idxInput,  int[] offInput,  Array output, Index idxOutput, int[] offOutput, int dim, int[] len) {

		final boolean bottom = (dim == input.getRank() - 1);

		int min = offInput[dim];
		int max = min + len[dim];

		for(int i = min ; i < max ; i++) {

			int o = offOutput[dim] + i - min;				//convert i (input Array) to o (output Array)
			idxInput.setDim(dim, i);
			idxOutput.setDim(dim, o);

			if(bottom) {
				output.setInt(idxOutput, input.getInt(idxInput));
			}
			else {
				copyInt(input, idxInput, offInput, output, idxOutput, offOutput, dim+1, len);
			}
		}
	}
	
	private static void copyFloat(Array input,  Index idxInput,  int[] offInput,  Array output, Index idxOutput, int[] offOutput, int dim, int[] len) {
		
//		System.out.println("copy float " + Arrays.toString(offInput) + " / " + Arrays.toString(offOutput) + " / dim=" + dim + " / " + Arrays.toString(len));

		final boolean bottom = (dim == input.getRank() - 1);
		
		int min = offInput[dim];
		int max = min + len[dim];
		
		for(int i = min ; i < max ; i++) {
			
			int o = offOutput[dim] + i - min;				//convert i (input Array) to o (output Array)
			idxInput.setDim(dim, i);
			idxOutput.setDim(dim, o);
			
			if(bottom) {
				output.setFloat(idxOutput, input.getFloat(idxInput));
			}
			else {
				copyFloat(input, idxInput, offInput, output, idxOutput, offOutput, dim+1, len);
			}
		}
		
//		if(dim == input.getRank() - 1) {
//			for(int i = offInput[dim] ; i < lenInput[dim] ; i++) {
//				int o = offOutput[dim] + i - offInput[dim];				//convert i (input Array) to o (output Array)
//				idxInput.setDim(dim, i);
//				idxOutput.setDim(dim, o);
//				output.setFloat(idxOutput, input.getFloat(idxInput));
//			}
//		}
//		else {
//			for(int i = offInput[dim] ; i < lenInput[dim] ; i++) {
//				int o = offOutput[dim] + i - offInput[dim];
//				idxInput.setDim(dim, i);
//				idxOutput.setDim(dim, o);
//				copyFloat(input, idxInput, offInput, lenInput, output, idxOutput, offOutput, lenOutput, dim+1);
//			}
//		}
		
	}
	
//	public Shard(Array array, int[] chunkShape) {
//		this(array,chunkShape,1.0,0.0);
//	}
	
	public Shard(Array array, int[] chunkShape) { //, double scaleFactor, double addOffset) {

		this.array = array;
		this.shape = array.getShape();
		
		if(shape.length != chunkShape.length) {
			throw new IllegalArgumentException();
		}
		
		this.chunkShape = chunkShape;
		this.numChunk = new int[chunkShape.length];
		for(int i = 0 ; i < chunkShape.length ; i++) {
			int size = shape[i];
			int csize = chunkShape[i];
			this.numChunk[i] = size  % csize  == 0 ? size  / csize  : size  / csize  + 1;
		}
//		this.scaleFactor = scaleFactor;
//		this.addOffset = addOffset;
	}
	
	public Array getArray() {
		return this.array;
	}
	
	public int[] getShape() {
		return this.array.getShape();
	}
	
	public DataType getDataType() {
		return this.array.getDataType();
	}
	
	public int[] getChunkShape() {
		return this.chunkShape;
	}
	
	public int[] getNumChunk() {
		return this.numChunk;
	}

	public int getNumChunk(int dim) {
		return this.numChunk[dim];
	}
	
	public Chunk getChunk(int[] chunk, DataType dataType) {
		
		int[] shape = this.array.getShape();
		
		int[] offInput = new int[shape.length];
		int[] offOutput = new int[shape.length];
		int[] len = new int[shape.length];
		
	    for(int d = 0 ; d < this.array.getRank() ; d++) {
	    	
	    	int iMin = chunk[d] * this.chunkShape[d];
	    	int iMax = iMin + this.chunkShape[d] - 1;
	    	
	    	if(iMax > shape[d]) {
	    		iMax = shape[d] - 1;
	    	}
	    	
	    	
	    	offInput[d] = iMin; 
	    	offOutput[d] = 0;
	    	len[d] = iMax - iMin + 1; // + 1;
	    }
	    
	    Index idxInput = this.array.getIndex();
	    Array output = Array.factory(dataType, this.chunkShape);
	    Index idxOutput = output.getIndex();
	    
	    if( (dataType == DataType.BYTE)  || (dataType == DataType.SHORT)  || (dataType == DataType.INT) ||
	    	(dataType == DataType.UBYTE) || (dataType == DataType.USHORT) || (dataType == DataType.UINT) ) {
	    	copyInt(this.array, idxInput, offInput, output, idxOutput, offOutput, 0, len);
	    	return new Chunk(output);
	    }
	    else if(dataType == DataType.FLOAT || dataType == DataType.DOUBLE) {
	    	copyFloat(this.array, idxInput, offInput, output, idxOutput, offOutput, 0, len);
	    	return new Chunk(output);
	    }
	    else {
	    	throw new IllegalArgumentException("Unsupported DataType " + dataType.toString());
	    }
	    
	}
	
}

