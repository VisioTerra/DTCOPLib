package fr.visioterra.lib.format.dtcop.chunk;

import java.util.List;

import fr.visioterra.lib.format.dtcop.huffman.Histogram;
import fr.visioterra.lib.format.dtcop.huffman.Huffman;
import fr.visioterra.lib.format.dtcop.quantization.QuantChunk;
import fr.visioterra.lib.io.bit.BitWriter;
import fr.visioterra.lib.io.stream.ByteArrayOutputStream;

public class ChunkWriter {

	private static final int minValue = -32767;
	private static final int maxValue = +32767;
	private static final int rleCode  = -32768;
	private static final int maxZeroSequenceLen = 22;
	
	private final int[] shape;
	private final float[] quantPolynom;
	private final int[] coefficients;
	
	private static QuantChunk checkError(Chunk origin, Chunk dct, List<QuantChunk> qChunks, double maxError) {
		
		for(QuantChunk qc : qChunks) {
			Chunk tmp = dct.copy();
			tmp.roundTripScale(qc);
			tmp.idct();
			double diff = tmp.getMaxAbsDiff(origin);
			if(diff < maxError) {
				return qc;
			}
		}
		
		return null;
	}
	
	public ChunkWriter(Chunk origin, List<QuantChunk> qChunks, double maxError, Cell[] order, Histogram histogram) {
	
		this.shape = origin.getShape();
		
		//create copy and apply 32x32x32 DCT
		Chunk dct = origin.copy();
		dct.dct();
		
		//find the right quantization matrix
		QuantChunk qc = checkError(origin, dct, qChunks, maxError);
		this.quantPolynom = qc.getPolynom();
		
//		System.out.println(Arrays.toString(idxChunk) + " => " + qc.getIdx() + " / " + Arrays.toString(qc.getPolynom()) + " / " + qc.getError() + " / " + maxError);
		
		//apply quantization matrix
		dct.scale(qc,false,true);
		
		//get values in zigzag order
		this.coefficients = dct.getAsIntArray(order);
		
		int zeroSequenceLen = 0;
		
		//browse value to update histogram and ignore DC coefficient
		for(int idx = 1 ; idx < this.coefficients.length ; idx++) {
			
			int sample = this.coefficients[idx];
			
			if(sample < minValue || maxValue < sample) {
				throw new IllegalArgumentException("value " + sample + " out of range [" + minValue + "," + maxValue + "]");
			}
			
			//case sample == 0 => increment count
			if(sample == 0) {
				zeroSequenceLen++;
			}
			else {
				//manage zero sequence
				if(zeroSequenceLen > 0) {
					if(zeroSequenceLen > maxZeroSequenceLen) {
						histogram.update(rleCode);
					} else {
						histogram.update(0,zeroSequenceLen);
					}
					zeroSequenceLen = 0;
				}
				histogram.update(sample);
			}
		}
		
		//manage reminded zero sequence
		if(zeroSequenceLen > 0) {
			if(zeroSequenceLen > maxZeroSequenceLen) {
				histogram.update(rleCode);
			} else {
				histogram.update(0,zeroSequenceLen);
			}
			zeroSequenceLen = 0;
		}
		
//		return values;
	}
	
	public byte[] getBytes(Huffman huffman) throws Exception {
		
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		
		try(BitWriter bw = new BitWriter(baos,64)) {
			
			int[] values = this.coefficients;
			int dc = values[0];
			
			//write shape
			bw.writeBits(this.shape.length,8);
			bw.writeBits(this.shape[0],8);
			bw.writeBits(this.shape[1],8);
			bw.writeBits(this.shape[2],8);
			
			//write polynom
			bw.writeBits(this.quantPolynom.length,8);
			for(int i = 0 ; i < this.quantPolynom.length ; i++) {
				int f = Float.floatToIntBits(this.quantPolynom[i]);
				bw.writeBits(f,32);
			}
			
			//write DC coefficient
			bw.writeBits(dc,32);
			
			//write AC coefficients
			int zeroSequenceLen = 0;
			for(int idx = 1 ; idx < values.length ; idx++) {
				
				int ac = values[idx];
				
				if(ac == 0) {
					zeroSequenceLen++;
				}
				else {
					//manage zero sequence
					if(zeroSequenceLen > 0) {
						if(zeroSequenceLen > maxZeroSequenceLen) {
							huffman.writeCode(bw,rleCode);
							bw.writeBits(zeroSequenceLen,16);
						}
						else {
							huffman.writeCode(bw,0,zeroSequenceLen);
						}
						zeroSequenceLen = 0;
					}
					
					//write current coef
					huffman.writeCode(bw,ac);
				}
			}
			
			//manage reminded zero sequence
			if(zeroSequenceLen > 0) {
				if(zeroSequenceLen > maxZeroSequenceLen) {
					huffman.writeCode(bw,rleCode);
					bw.writeBits(zeroSequenceLen,16);
				}
				else {
					huffman.writeCode(bw,0,zeroSequenceLen);
				}
				zeroSequenceLen = 0;
			}
		
		}
		
		return baos.toByteArray();
	}
	
}
