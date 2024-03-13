package fr.visioterra.lib.io.bit;

import java.io.OutputStream;
import java.util.Arrays;

public class BitWriter implements AutoCloseable {

	private final OutputStream os;
	private final byte[] buffer;
	
	private int emptyBits;
	private int bufIndex;
	
	public BitWriter(OutputStream os, int bufLength) {
		this.os = os;
		this.buffer = new byte[bufLength];
		
		this.emptyBits = 8;
		this.bufIndex = 0;
	}

	private void flush(int len) throws Exception {
		os.write(this.buffer, 0, len);
		
		// Reset the bytes buffer index
		bufIndex = 0;
		Arrays.fill(this.buffer, 0, len, (byte) 0x00);
	}
	
	public void writeBits(int code, int codeLen) throws Exception {
		
		if (emptyBits == 0) {
			if (++bufIndex >= buffer.length) flush(buffer.length);
			emptyBits = 8;
		}
		
		// If we have enough space for the code
		if (codeLen < emptyBits) {
			int mask = (1 << codeLen) - 1;
			buffer[bufIndex] |= ((code & mask) << (emptyBits - codeLen));
			emptyBits -= codeLen;
		}
		else { // Otherwise
			// write remaining bits available
			buffer[bufIndex] |= ((code >>> (codeLen - emptyBits)) & ((1 << emptyBits) - 1));
			if (++bufIndex >= buffer.length) flush(buffer.length);
			
			// update codeLen
			codeLen -= emptyBits;

			// write middle of the code if there is more than 8bits left
			emptyBits = 8;
			while (codeLen > 8) {
				buffer[bufIndex] = (byte) ((code >>> (codeLen - 8)) & 0xff);
				if (++bufIndex >= buffer.length) flush(buffer.length);
				
				codeLen -= emptyBits;
			}

			// write end of the code
			int mask = (1 << codeLen) - 1;
			buffer[bufIndex] |= ((code & mask) << (emptyBits - codeLen));
			emptyBits -= codeLen;
		}
	}
	
	public void flush() throws Exception {
		if (bufIndex > 0 || this.emptyBits != 8) {
			flush(bufIndex+1);
			this.emptyBits = 8;
		}
	}
	
	@Override public void close() throws Exception {
		flush();
		os.close();
	}
	
}
