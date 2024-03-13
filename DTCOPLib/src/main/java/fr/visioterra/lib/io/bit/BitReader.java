package fr.visioterra.lib.io.bit;

import java.io.InputStream;

public class BitReader implements AutoCloseable {
	
	private final InputStream is;
	private long buffer;
	private int bitPos;
	
	
	public BitReader(InputStream is) throws Exception {
		this.is = is;
		this.buffer = this.is.read();
		
		if(this.buffer < 0) {
			throw new IllegalArgumentException("EOF reached");
		}
		
		this.bitPos = 8;
	}
	
	public long readBits(int len) throws Exception {
		
//		System.out.println("readBits(" + len + ")");

		if(len > 64-8) {
			throw new IllegalArgumentException("Cannot read more than 56 bits at once");
		}
		
//		System.out.println("buffer = " + Long.toBinaryString(this.buffer) + "  /  bitPos = " + this.bitPos);
		
		while(len > this.bitPos) {
			
			long r = this.is.read();
			if(r < 0) {
				throw new IllegalArgumentException("EOF reached");	
			}
			this.buffer = (this.buffer << 8) | r;
			this.bitPos += 8;
			
//			System.out.println("buffer = " + Long.toBinaryString(this.buffer) + "  /  bitPos = " + this.bitPos + " (read 1 byte)");
		}
		
		long mask = (0x0000000000000001L << len) - 1;
		long r = (this.buffer >> (this.bitPos - len)) & mask;
		this.bitPos = this.bitPos - len;
		
//		System.out.println("buffer >> len = " + Long.toBinaryString(this.buffer >> len) + "  /  mask = " + Long.toBinaryString(mask) + "  /  r = " + r + "  /  bitPos = " + this.bitPos);
		
		return r;
		
	}
	
	@Override public void close() throws Exception {
		this.is.close();
	}
	
}
