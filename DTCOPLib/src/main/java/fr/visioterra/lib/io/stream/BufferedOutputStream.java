package fr.visioterra.lib.io.stream;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class BufferedOutputStream extends FilterOutputStream {
	
    protected byte buf[];
    protected int count;
    
    public BufferedOutputStream(OutputStream out) {
        this(out, 32*1024);
    }

    public BufferedOutputStream(OutputStream out, int size) {
        super(out);
        if (size <= 0) {
            throw new IllegalArgumentException("Buffer size <= 0");
        }
        buf = new byte[size];
    }

    private void flushBuffer() throws IOException {
        if (count > 0) {
            out.write(buf, 0, count);
//            System.out.println("flushBuffer(...) - " + count);
            count = 0;
        }
    }

    public void write(int b) throws IOException {
        if (count >= buf.length) {
            flushBuffer();
        }
        buf[count++] = (byte)b;
    }

    public void write(byte b[], int off, int len) throws IOException {
//    	System.out.println("write(byte[],int,int)  " + len);
        if (len >= buf.length) {
            /* If the request length exceeds the size of the output buffer,
               flush the output buffer and then write the data directly.
               In this way buffered streams will cascade harmlessly. */
            flushBuffer();
            out.write(b, off, len);
            return;
        }
        if (len > buf.length - count) {
            flushBuffer();
        }
        System.arraycopy(b, off, buf, count, len);
        count += len;
    }

    public void flush() throws IOException {
        flushBuffer();
        out.flush();
    }
}