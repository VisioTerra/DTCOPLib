package com.bc.zarr.compressor;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;

import com.bc.zarr.Compressor;

public class ZlibCompressor extends Compressor {
	
    private final int level;

    public ZlibCompressor(Map<String, Object> map) {
        final Object levelObj = map.get("level");
        if (levelObj == null) {
            this.level = 1; //default value
        } else if (levelObj instanceof String) {
            this.level = Integer.parseInt((String) levelObj);
        } else {
            this.level = ((Number) levelObj).intValue();
        }
        validateLevel();
    }

    @Override public String toString() {
        return "compressor=" + getId() + "/level=" + level;
    }
    
    @Override public String toShortString() {
    	return getId() + "-" + level;
    }

    private void validateLevel() {
        // see new Deflater().setLevel(level);
        if (level < 0 || level > 9) {
            throw new IllegalArgumentException("Invalid compression level: " + level);
        }
    }

    @Override public String getId() {
        return "zlib";
    }

    // this getter is needed for JSON serialisation
    public int getLevel() {
        return level;
    }

    @Override public void compress(InputStream is, OutputStream os) throws IOException {
        try (final DeflaterOutputStream dos = new DeflaterOutputStream(os, new Deflater(level))) {
            passThrough(is, dos);
        }
    }

    @Override public void uncompress(InputStream is, OutputStream os) throws IOException {
        try (final InflaterInputStream iis = new InflaterInputStream(is, new Inflater())) {
            passThrough(iis, os);
        }
    }
}
