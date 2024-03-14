package fr.visioterra.lib.format.dtcop.zarr;

import java.io.EOFException;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteOrder;
import java.util.Map;

import javax.imageio.stream.ImageOutputStream;
import javax.imageio.stream.MemoryCacheImageInputStream;
import javax.imageio.stream.MemoryCacheImageOutputStream;

import com.bc.zarr.Compressor;

import fr.visioterra.lib.io.stream.ByteArrayInputStream;
import fr.visioterra.lib.io.stream.ByteArrayOutputStream;
import fr.visioterra.lib.io.stream.StreamTools;
import fr.visioterra.lib.tools.Benchmark;
import io.airlift.compress.zstd.ZstdInputStream;
import io.airlift.compress.zstd.ZstdOutputStream;
import ucar.ma2.Array;
import ucar.ma2.DataType;
import ucar.ma2.IndexIterator;

public class QuantCompressor extends Compressor {
	
	private final double quant;
	
	public final Benchmark bench1 = new Benchmark("alloc");
	public final Benchmark bench2 = new Benchmark("read");
	public final Benchmark benchZ = new Benchmark("zstd");
	public final Benchmark bench3 = new Benchmark("short[] to Array");
	
	public QuantCompressor(Map<String, Object> map) {
		
//		this(6000);
        final Object q = map.get("quant");
        if (q == null) {
            this.quant = 10; //default value
        } else if (q instanceof String) {
            this.quant = Integer.parseInt((String)q);
        } else {
            this.quant = ((Number)q).intValue();
        }
        
        System.out.println("QuantCompressor - quant = " + this.quant);
	}
	
	public QuantCompressor(double quant) {
		this.quant = quant;
	}
	
	@Override public String getId() {
		return "quant";
	}

	@Override public String toString() {
		return "compressor=" + getId();
	}
	
	@Override public String toShortString() {
		return getId();
	}

	@Override public void compress(InputStream is, OutputStream os) throws IOException {
		
		throw new UnsupportedOperationException();
		
//		try(DataInputStream dis = new DataInputStream(is); DataOutputStream dos = new DataOutputStream(new ZstdOutputStream(os))) {
//			
//			boolean loop = true;
//			while(loop) {
//				dis.readShort()
//			}
//			
//	        final byte[] bytes = new byte[4096];
//	        int read = is.read(bytes);
//	        while (read >= 0) {
//	            if (read > 0) {
//	                os.write(bytes, 0, read);
//	            }
//	            read = is.read(bytes);
//	        }
//			
//		}
	}

	@Override public void uncompress(InputStream is, OutputStream os) throws IOException {
		
		throw new UnsupportedOperationException();
		
//		try (ZstdInputStream zis = new ZstdInputStream(is)) {
//			passThrough(zis, os);
//		}
	}
	
    @Override public boolean canHandleArray() {
    	return true;
    }
    
    public void compress1(Array array, OutputStream os) throws IOException {
    	
    	final short[] shorts = (short[]) array.get1DJavaArray(DataType.SHORT);
    	
    	for(int i = 0 ; i < shorts.length ; i++) {
    		shorts[i] = (short)Math.round(shorts[i] / quant);
    	}
    	
    	try(ZstdOutputStream zos = new ZstdOutputStream(os); ImageOutputStream ios = new MemoryCacheImageOutputStream(zos)) {
    		ios.writeShorts(shorts,0,shorts.length);
    	}
    	
    }
    
    public Array uncompress1(InputStream is, Array array) throws IOException {
    	
    	try(MemoryCacheImageInputStream mciis = new MemoryCacheImageInputStream(new ZstdInputStream(is))) {
    		
    		IndexIterator idxIter = array.getIndexIterator();
    		
    		while(idxIter.hasNext()) {
    			int i = (int)Math.round(mciis.readShort() * this.quant);
    			
    			if(i < Short.MIN_VALUE) {
    				i = Short.MIN_VALUE;
    			}
    			else if(i > Short.MAX_VALUE) {
    				i = Short.MAX_VALUE;
    			}
    			
//    			idxIter.setShortCurrent((short)i);
    			idxIter.setShortNext((short)i);
    		}
    	}
    	
    	return array;
    }
    
    
    @Override public void compress(Array array, OutputStream os) throws IOException {

		int min = Integer.MAX_VALUE;
		int max = Integer.MIN_VALUE;
    	
    	{

    		IndexIterator idxIter = array.getIndexIterator();
    		int[] histo = new int[64*1024];
    		long sum = 0;
    		int count = 0;
    		int values = 0;

    		while(idxIter.hasNext()) {

    			int value = idxIter.getShortNext();

    			sum += value;
    			count++;

    			if(value < min) {
    				min = value;
    			}

    			if(value > max) {
    				max = value;
    			}

    			int occurence = histo[value - Short.MIN_VALUE];
    			if(occurence == 0) {
    				values++;
    			}
    			histo[value - Short.MIN_VALUE]++;
    		}

//    		System.out.println("before compress(...) : average = " + (sum / count) +  " / min/max = " + min + "/" + max + " / diff min/max = " + (max-min) + " / values = " + values);
    	}
    	
    	
    	short[] shorts = new short[(int)array.getSize()];
    	int idx = 0;

//    	double a = (max - min) / this.quant;
    	
    	IndexIterator idxIter = array.getIndexIterator();
    	while(idxIter.hasNext()) {
    		int value = idxIter.getShortNext();
    		int d = (int)Math.round(value / this.quant);
//    		value = (int)Math.round((d + 0.5) * this.quant);
    		value = (int)Math.round(d * this.quant);
    		shorts[idx++] = (short)value;
    	}


    	{
    		min = Integer.MAX_VALUE;
    		max = Integer.MIN_VALUE;
    		int[] histo = new int[64*1024];
    		long sum = 0;
    		int count = 0;
    		int values = 0;

    		for(short s : shorts) {

    			int value = s;

    			sum += value;
    			count++;

    			if(value < min) {
    				min = value;
    			}

    			if(value > max) {
    				max = value;
    			}

    			int occurence = histo[value - Short.MIN_VALUE];
    			if(occurence == 0) {
    				values++;
    			}
    			histo[value - Short.MIN_VALUE]++;
    		}

//    		System.out.println("after  compress(...) : average = " + (sum / count) +  " / min/max = " + min + "/" + max + " / diff min/max = " + (max-min) + " / values = " + values);
    	}
    	
    	
//    	final short[] shorts = (short[]) array.get1DJavaArray(DataType.SHORT);
//    	shorts = (short[]) array.get1DJavaArray(DataType.SHORT);
    	
//    	for(int i = 0 ; i < shorts.length ; i++) {
//    		shorts[i] = (short)Math.round(shorts[i] / quant);
//    	}
    	
    	try(ZstdOutputStream zos = new ZstdOutputStream(os); ImageOutputStream ios = new MemoryCacheImageOutputStream(zos)) {
    		ios.writeShorts(shorts,0,shorts.length);
    	}
    	
    }
    
//    public int count = 0;
    
    public Array uncompress(InputStream is, Array array) throws IOException {
    	
//    	count++;
//    	System.out.println("QuantCompressor(is,array) : " + count + "  ----------------------------------------------");
		
    	/*
		if(is instanceof FileInputStream) {
			
			bench1.start();
			byte[] bytes = null;
			bench1.stop();
			
			
			bench2.start();
			try(ByteArrayOutputStream baos = new ByteArrayOutputStream(16*1024)) {
				IOTools.copy(is,baos,4*1024);
				bytes = baos.toByteArray();
			}
			bench2.stop();
			
			
			benchZ.start();
			try(ByteArrayOutputStream baos = new ByteArrayOutputStream(576*1024);
					ZstdInputStream zis = new ZstdInputStream(new ByteArrayInputStream(bytes)) ) {
				IOTools.copy(zis, baos, 32*1024);
				bytes = baos.toByteArray();
			}
			benchZ.stop();
			
			
			bench3.start();
			IndexIterator idxIter = array.getIndexIterator();
			int boff = 0;
    		while(idxIter.hasNext()) {
    			int b0 = bytes[boff];
                int b1 = bytes[boff + 1] & 0xff;
                idxIter.setShortNext((short)((b0 << 8) | b1));
                boff += 2;
    		}
			bench3.stop();
			
		}
		*/
		
		if(is instanceof FileInputStream) {
			
			bench1.start();
			byte[] bytes = new byte[64*64*64*2];
			bench1.stop();
			
			bench2.start();
			benchZ.start();
			try(ZstdInputStream zis = new ZstdInputStream(is) ) {
//			try(ZstdInputStream zis = new ZstdInputStream(new BufferedInputStream(is,16*1024)) ) {
				int pos = 0;
				int len = zis.read(bytes,pos,bytes.length - pos);
				while(len > 0) {
					pos += len;
					len = zis.read(bytes,pos,bytes.length - pos);
				}
			}
			bench2.stop();
			benchZ.stop();
			
			bench3.start();
			IndexIterator idxIter = array.getIndexIterator();
			int boff = 0;
    		while(idxIter.hasNext()) {
    			int b0 = bytes[boff];
                int b1 = bytes[boff + 1] & 0xff;
                idxIter.setShortNext((short)((b0 << 8) | b1));
                boff += 2;
    		}
			bench3.stop();
			
		}
		else try(MemoryCacheImageInputStream mciis = new MemoryCacheImageInputStream(new ZstdInputStream(is))) {

			bench1.start();
			short[] shorts = new short[(int)array.getSize()]; 
			bench1.stop();
			
    		bench2.start();
    		benchZ.start();
    		mciis.readFully(shorts, 0, shorts.length);
    		bench2.stop();
    		benchZ.stop();
    		
    		bench3.start();
    		IndexIterator idxIter = array.getIndexIterator();
    		
    		int idx = 0;
    		while(idxIter.hasNext()) {
    			idxIter.setShortNext(shorts[idx++]);
    		}
    		bench3.stop();
    		
    		
    		/*
    		IndexIterator idxIter = array.getIndexIterator();
    		
    		while(idxIter.hasNext()) {
//    			int i = (int)Math.round(mciis.readShort() * this.quant);
//    			
//    			if(i < Short.MIN_VALUE) {
//    				i = Short.MIN_VALUE;
//    			}
//    			else if(i > Short.MAX_VALUE) {
//    				i = Short.MAX_VALUE;
//    			}
//    			
////    			idxIter.setShortCurrent((short)i);
//    			idxIter.setShortNext((short)i);
    			
    			idxIter.setShortNext(mciis.readShort());
    		}
    		*/
    	}
    	
    	return array;
    }
    
    public Array uncompress3(InputStream is, Array array) throws IOException {

    	bench1.start();
    	byte[] input = null;
    	try(ByteArrayOutputStream baos = new ByteArrayOutputStream(4*1024)) {
    		StreamTools.copy(is, baos, 4*1024);
    		input = baos.toByteArray();
    	}
    	bench1.stop();
    	
    	
    	
    	bench2.start();
    	byte[] b = new byte[(int)array.getSizeBytes()];
    	try(ZstdInputStream zis = new ZstdInputStream(new ByteArrayInputStream(input))) {
    		int off = 0;
    		int len = b.length;
            while (len > 0) {
                int nbytes = zis.read(b, off, len);
                if (nbytes == -1) {
                    throw new EOFException();
                }
                off += nbytes;
                len -= nbytes;
            }
    	}
    	bench2.stop();
    	
    	
    	
    	bench3.start();
    	{
    		IndexIterator idxIter = array.getIndexIterator();
    		int idx = 0;
    		while(idxIter.hasNext()) {
    			int b0 = b[idx];
    			int b1 = b[idx + 1] & 0xff;
    			idxIter.setShortNext((short)((b0 << 8) | b1));
    			idx += 2;
    		}
    	}
    	bench3.stop();
    	
    	return array;
    }
    
}
