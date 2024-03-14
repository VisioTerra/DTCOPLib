package fr.visioterra.lib.io.stream;

import java.io.IOException;

public interface AtomicSeekableInputStream {
	
	 public byte[] read(long position, byte[] b, int off, int len) throws IOException; 
	
}
