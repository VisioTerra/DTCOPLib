/*
 *
 * MIT License
 *
 * Copyright (c) 2020. Brockmann Consult GmbH (info@brockmann-consult.de)
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */
package com.bc.zarr.chunk;

import com.bc.zarr.Compressor;
import com.bc.zarr.storage.Store;
import ucar.ma2.Array;
import ucar.ma2.DataType;

import javax.imageio.stream.ImageInputStream;
import javax.imageio.stream.ImageOutputStream;
import javax.imageio.stream.ImageOutputStreamImpl;
import javax.imageio.stream.MemoryCacheImageInputStream;
import javax.imageio.stream.MemoryCacheImageOutputStream;
import java.io.*;
import java.nio.ByteOrder;

public class ChunkReaderWriterImpl_Short extends ChunkReaderWriter {

    public ChunkReaderWriterImpl_Short(ByteOrder order, Compressor compressor, int[] chunkShape, Number fill, Store store) {
        super(order, compressor, chunkShape, fill, store);
    }

    @Override public Array read(String storeKey) throws IOException {
    
//    	System.out.println("ChunkReaderWriterImpl_Short.read(" + storeKey + ")");
    	
    	
    	if(readVersion == 1) {
    		if(this.compressor.canHandleStore()) {
    			return this.compressor.uncompress(this.store, storeKey, DataType.SHORT, chunkShape);
    		}
    		else if(this.compressor.canHandleArray()) {
    			try (InputStream is = store.getInputStream(storeKey)) {
    				if(is != null) {
    					return this.compressor.uncompress(is,Array.factory(DataType.SHORT, chunkShape));
    				} else {
    					return createFilled(DataType.SHORT);
    				}
    			}
    		}
    		else {
    			try (InputStream is = store.getInputStream(storeKey)) {
    				if(is == null) {
    					return createFilled(DataType.SHORT);
    				}
    			}
    			throw new IOException("Not implemented");
    			
    		}
    	}
    	else {
    		try (InputStream is = store.getInputStream(storeKey)) {
    			if (is != null) {
    				try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
    					compressor.uncompress(is, os);
    					final short[] shorts = new short[getSize()];
    					try (	final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray());
    							final ImageInputStream iis = new MemoryCacheImageInputStream(bais)) {
    						iis.setByteOrder(order);
    						iis.readFully(shorts, 0, shorts.length);
    					}
    					return Array.factory(DataType.SHORT, chunkShape, shorts);
    				}
    			} else {
    				return createFilled(DataType.SHORT);
    			}
    		}
    	}
    	
    }

    
    private static class BufferedImageOutputStream extends ImageOutputStreamImpl {

    	private final fr.visioterra.lib.io.stream.ByteArrayOutputStream baos;
    	
    	public BufferedImageOutputStream() {
    		this.baos = new fr.visioterra.lib.io.stream.ByteArrayOutputStream();
    	}
    	
		@Override public int read() throws IOException {
			throw new IOException("Not implemented");
		}
		
		@Override public int read(byte[] b, int off, int len) throws IOException {
			throw new IOException("Not implemented");
		}

		@Override public void write(int b) throws IOException {
//			System.out.println("BufferedImageOutputStream.write(int)");
			this.baos.write(b);
		}
		
		@Override public void write(byte[] b, int off, int len) throws IOException {
//			System.out.println("BufferedImageOutputStream.write(byte[],off," + len + ")");
			this.baos.write(b, off, len);
		}
		
		public byte[] toByteArray() throws IOException {
			this.flush();
			return this.baos.toByteArray();
		}
		
		public InputStream asInputStream() throws IOException {
			this.flush();
			byte[] buf = this.baos.toByteArray();
			return new fr.visioterra.lib.io.stream.ByteArrayInputStream(buf);
		}
    	
    }
    
    
    @Override public void write(String storeKey, Array array) throws IOException {
    	
//    	System.out.println("ChunkReaderWriterImpl_Short.write(" + storeKey + ")");
    	
    	if(writeVersion == 1) {
    		if(this.compressor.canHandleArray()) {
        		try (final OutputStream os = store.getOutputStream(storeKey)) {
        			this.compressor.compress(array, os);
        		}
    		}
    		else {
//    			throw new IOException("Not implemented");
    			
    			short[] shorts = (short[]) array.get1DJavaArray(DataType.SHORT);
    			
    			try(	BufferedImageOutputStream bios = new BufferedImageOutputStream();
    					OutputStream os = store.getOutputStream(storeKey)) {
    				bios.setByteOrder(order);
        			bios.writeShorts(shorts, 0, shorts.length);
        			compressor.compress(bios.asInputStream(), os);
    			}
    				
    		}
    	}
    	else {
    		try (	final ImageOutputStream iis = new MemoryCacheImageOutputStream(new ByteArrayOutputStream());
    				final InputStream is = new ZarrInputStreamAdapter(iis);
    				final OutputStream os = store.getOutputStream(storeKey)) {
    			final short[] shorts = (short[]) array.get1DJavaArray(DataType.SHORT);
    			iis.setByteOrder(order);
    			iis.writeShorts(shorts, 0, shorts.length);
    			iis.seek(0);
    			compressor.compress(is, os);
    		}
    	}
    	
    }
}
