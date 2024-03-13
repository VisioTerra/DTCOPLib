package fr.visioterra.lib.io.stream;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class StreamTools {
	
	public static short readShort(InputStream is) throws IOException {
		return (short)((is.read() << 8) | is.read()); 
	}
	
	public static int readInt(InputStream is) throws IOException {
		return (is.read() << 24) | (is.read() << 16) | (is.read() << 8) | (is.read());
	}
	
	public static void readFully(InputStream is, byte b[], int off, int len) throws IOException {
        int n = 0;
        do {
            int count = is.read(b, off + n, len - n);
            if (count < 0)
                throw new EOFException();
            n += count;
        } while (n < len);
    }
	
	
	public static void writeShort(OutputStream os, int s) throws IOException {
		os.write((byte)(s >>> 8));
		os.write((byte)(s >>> 0));
	}
	
	public static void writeInt(OutputStream os, int s) throws IOException {
		os.write((byte)(s >>> 24));
		os.write((byte)(s >>> 16));
		os.write((byte)(s >>> 8));
		os.write((byte)(s >>> 0));
	}
	
	
}
