package com.bc.zarr.compressor;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.bc.zarr.Compressor;

public class NullCompressor extends Compressor {

    @Override
    public String getId() {
        return null;
    }

    @Override
    public String toString() {
        return getId();
    }
    
    @Override public String toShortString() {
    	return "none";
    }

    @Override
    public void compress(InputStream is, OutputStream os) throws IOException {
        passThrough(is, os);
    }

    @Override
    public void uncompress(InputStream is, OutputStream os) throws IOException {
        passThrough(is, os);
    }

}
