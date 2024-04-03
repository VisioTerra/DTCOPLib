package fr.visioterra.lib.format.dtcop.zarr;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

import com.bc.zarr.Compressor;

import fr.visioterra.lib.format.dtcop.quantization.QuantChunk;
import fr.visioterra.lib.format.dtcop.shard.Shard;
import fr.visioterra.lib.format.dtcop.shard.ShardWriter;
import fr.visioterra.lib.io.stream.BufferedOutputStream;
import ucar.ma2.Array;

public class DTCOPCompressor extends Compressor {
	
	private final int[] chunkShape;
	private final double maxError;
	private final int threadNumber;
	private final ArrayList<QuantChunk> qChunks;
	
	
	public DTCOPCompressor(Map<String, Object> map, double maxError, int threadNumber) {
		
		this.chunkShape = new int[] {32,32,32};
		this.maxError = maxError;
		this.threadNumber = threadNumber;
		this.qChunks = new ArrayList<>();
		
		double sqrt2 = Math.sqrt(2);
		float chunkScale = (float)Math.pow(sqrt2,15);
		
		qChunks.add(new QuantChunk(chunkShape, new float[] {chunkScale * 3.0f, 40.0f, 50.0f}, qChunks.size()));
		qChunks.add(new QuantChunk(chunkShape, new float[] {chunkScale * 3.0f, 30.0f, 40.0f}, qChunks.size()));
		
		qChunks.add(new QuantChunk(chunkShape, new float[] {chunkScale * 2.0f, 30.0f, 30.0f}, qChunks.size()));
		qChunks.add(new QuantChunk(chunkShape, new float[] {chunkScale * 2.0f, 25.0f, 20.0f}, qChunks.size()));
		qChunks.add(new QuantChunk(chunkShape, new float[] {chunkScale * 2.0f, 20.0f, 10.0f}, qChunks.size()));
		
		qChunks.add(new QuantChunk(chunkShape, new float[] {chunkScale * 1.5f, 20.0f       }, qChunks.size()));
		qChunks.add(new QuantChunk(chunkShape, new float[] {chunkScale * 1.5f, 15.0f       }, qChunks.size()));
		qChunks.add(new QuantChunk(chunkShape, new float[] {chunkScale * 1.5f, 10.0f       }, qChunks.size()));
		
		qChunks.add(new QuantChunk(chunkShape, new float[] {chunkScale * 1.0f, 8.0f        }, qChunks.size()));		//TODO : check AC coef between -2^15 , +2^15
		qChunks.add(new QuantChunk(chunkShape, new float[] {chunkScale * 1.0f, 4.0f        }, qChunks.size()));		//TODO : check AC coef between -2^15 , +2^15
		qChunks.add(new QuantChunk(chunkShape, new float[] {chunkScale * 1.0f, 2.0f        }, qChunks.size()));		//TODO : check AC coef between -2^15 , +2^15
		qChunks.add(new QuantChunk(chunkShape, new float[] {chunkScale * 1.0f, 1.0f        }, qChunks.size()));		//TODO : check AC coef between -2^15 , +2^15
		qChunks.add(new QuantChunk(chunkShape, new float[] {chunkScale * 1.0f              }, qChunks.size()));		//TODO : check AC coef between -2^15 , +2^15
		
		qChunks.add(new QuantChunk(chunkShape, new float[] {chunkScale / 1.5f              }, qChunks.size()));		//TODO : check AC coef between -2^15 , +2^15
		qChunks.add(new QuantChunk(chunkShape, new float[] {chunkScale / 2.0f              }, qChunks.size()));		//TODO : check AC coef between -2^15 , +2^15
		qChunks.add(new QuantChunk(chunkShape, new float[] {chunkScale / 3.0f              }, qChunks.size()));		//TODO : check AC coef between -2^15 , +2^15
		qChunks.add(new QuantChunk(chunkShape, new float[] {chunkScale / 4.0f              }, qChunks.size()));		//TODO : check AC coef between -2^15 , +2^15
		qChunks.add(new QuantChunk(chunkShape, new float[] {chunkScale / 5.0f              }, qChunks.size()));		//TODO : check AC coef between -2^15 , +2^15
		
	}
	
	
	@Override public String getId() {
		return "dtcop";
	}

	@Override public String toString() {
		return "compressor=" + getId();
	}
    
	@Override public String toShortString() {
		return getId();
	}

	@Override public void compress(InputStream is, OutputStream os) throws IOException {
		throw new IllegalArgumentException("Not implemented");
	}

	@Override public void uncompress(InputStream is, OutputStream os) throws IOException {
		throw new IllegalArgumentException("Not implemented");
	}
    
	@Override public boolean canHandleArray() {
		return true;
	}
	
	@Override public void compress(Array array, OutputStream os) throws IOException {
		
		//TODO : remove debug
		System.out.println("compress array " + Arrays.toString(array.getShape()));

		Shard shard = new Shard(array,this.chunkShape);

		try(BufferedOutputStream bos = new BufferedOutputStream(os)) {
			ShardWriter.write(bos, shard, this.qChunks, this.maxError, threadNumber);
		} catch(Exception e) {
			throw new IOException(e);
		}
		
	}
    
	@Override public Array uncompress(InputStream is, Array array) throws IOException {
		throw new IllegalArgumentException("Not implemented");
	}
    
}
