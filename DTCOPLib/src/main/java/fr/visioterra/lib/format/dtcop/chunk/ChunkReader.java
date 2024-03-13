package fr.visioterra.lib.format.dtcop.chunk;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import fr.visioterra.lib.cache.KVCacheMap;
import fr.visioterra.lib.format.dtcop.huffman.Huffman;
import fr.visioterra.lib.format.dtcop.quantization.QuantChunk;
import fr.visioterra.lib.io.bit.BitReader;
import fr.visioterra.lib.io.stream.StreamTools;
import ucar.ma2.DataType;

public class ChunkReader {

	
	
	
	
	private static final int rleCode  = -32768;
	
	
	
    
    
	public static Chunk getChunk(InputStream is, Huffman huffman, Cell[] order) throws Exception {
		
		//TODO : check EOF
		
		//read chunk shape
		int[] shape = new int[is.read()];
		int size = 1;
		for(int d = 0 ; d < shape.length ; d++) {
			shape[d] = is.read();
			size = size * shape[d];
		}
		
		//read quantization polynom
		float[] quantPolynom = new float[is.read()];
		for(int d = 0 ; d < quantPolynom.length ; d++) {
			quantPolynom[d] = Float.intBitsToFloat(StreamTools.readInt(is));
		}
		
		//create output array for coefficients in zig zag order
		int[] zz = new int[size];
		
		//read DC coefficient
		zz[0] = StreamTools.readInt(is);
		
//		System.out.println("Size    = " + size);
//		System.out.println("Shape   = " + Arrays.toString(shape));
//		System.out.println("Polynom = " + Arrays.toString(quantPolynom));
//		System.out.println("DC coef = " + dc);
//		System.out.println("Shape   = " + Arrays.toString(shape) + " / Polynom = " + Arrays.toString(quantPolynom) + " / DC coef = " + zz[0]);  

		
		QuantChunk cq = new QuantChunk(shape, quantPolynom);
//		Cell[] order = Chunk.order(shape,true);
		
		
		try(BitReader br = new BitReader(is)) {
			
			//Start at idx = 1 because position 0 is already initialized with DC coef
			int idx = 1; 
			while(idx < size - 1) {
				
				try {
					zz[idx] = huffman.readSymbol(br);
				
					if(zz[idx] == rleCode) {
						int s = (short)br.readBits(16);
						for(int i = 0 ; i < s ; i++) {
							zz[idx] = 0;
							idx++;
						}
					}
					else {
						idx++;
					}

				} catch(Exception e) {
					System.out.println(idx + " / " + size);
					throw e;
				}
				
			}
		}
		
		Chunk chunk = new Chunk(DataType.FLOAT, shape, zz, order);
		chunk.scale(cq,true,false);
		chunk.idct();
		return chunk;
	}
	
	
}




















