package fr.visioterra.lib.format.dtcop.quantization;

import fr.visioterra.lib.format.dtcop.chunk.Chunk;
import ucar.ma2.Array;
import ucar.ma2.ArrayFloat;
import ucar.ma2.Index;

public class QuantChunk extends Chunk {
	
	private static Array quant(int[] shape, float[] polynom) {
		
		if(shape.length != 3) {
			throw new IllegalArgumentException("shape.length = " + shape.length);
		}
		
		Array output = new ArrayFloat(shape);
		Index index = output.getIndex();
		
		for (int k = 0; k < shape[0]; k++) {
			index.setDim(0, k);
			
			for (int j = 0; j < shape[1]; j++) {
				index.setDim(1, j);
				
				for (int i = 0; i < shape[2]; i++) {
					index.setDim(2, i);
					
					double dist = Math.sqrt(k*k + j*j + i*i);
					double x = 1.0;
					double sum = polynom[0];
					
					for(int c = 1 ; c < polynom.length ; c++) {
						x = x * dist;
						sum += x * polynom[c];
					}
					
					output.setFloat(index,(float)sum);
				}
			}
		}
		
		return output;
	}
	
	private final float[] polynom;
	private final int idx;
	
	public QuantChunk(int[] shape, float[] polynom) {
		this(shape,polynom,0);
	}
	
	public QuantChunk(int[] shape, float[] polynom, int idx) {
		super(quant(shape, polynom));
		this.polynom = polynom;
		this.idx = idx;
	}
	
	public float[] getPolynom() {
		return this.polynom;
	}
	
	public int getIdx() {
		return this.idx;
	}
	
}
