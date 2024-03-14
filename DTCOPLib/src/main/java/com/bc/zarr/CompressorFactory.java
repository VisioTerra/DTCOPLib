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

package com.bc.zarr;

import java.util.HashMap;
import java.util.Map;

import com.bc.zarr.compressor.BloscCompressor;
import com.bc.zarr.compressor.NullCompressor;
import com.bc.zarr.compressor.ZlibCompressor;

import fr.visioterra.lib.format.dtcop.zarr.DTCOPCompressor;


public class CompressorFactory {

    public final static Compressor nullCompressor = new NullCompressor();

    /**
     * @return the properties of the default compressor as a key/value map.
     */
    public static Map<String, Object> getDefaultCompressorProperties() {
        final Map<String, Object> map = new HashMap<>();

        /* zlib defaults */
//        map.put("id", "zlib");
//        map.put("level", 1);

        /* blosc defaults */
        map.put("id", "blosc");
        map.putAll(BloscCompressor.defaultProperties);

        return map;
    }

    /**
     * @return a new Compressor instance using the method {@link #create(Map properties)} with {@link #getDefaultCompressorProperties()}.
     */
    public static Compressor createDefaultCompressor() {
        return create(getDefaultCompressorProperties());
    }

    /**
     * Creates a new {@link Compressor} instance according to the given properties.
     *
     * @param properties a Map containing the properties to create a compressor
     * @return a new Compressor instance according to the properties
     * @throws IllegalArgumentException If it is not able to create a Compressor.
     */
    public static Compressor create(Map<String, Object> properties) {
        final String id = (String) properties.get("id");
        return create(id, properties);
    }

    /**
     * Creates a new {@link Compressor} instance according to the id and the given properties.
     *
     * @param id           the type of the compression algorithm
     * @param keyValuePair an even count of key value pairs defining the compressor specific properties
     * @return a new Compressor instance according to the id and the properties
     * @throws IllegalArgumentException If it is not able to create a Compressor.
     */
    public static Compressor create(String id, Object... keyValuePair) {
        if (keyValuePair.length % 2 != 0) {
            throw new IllegalArgumentException("The count of keyValuePair arguments must be an even count.");
        }
        return create(id, toMap(keyValuePair));
    }

    /**
     * Creates a new {@link Compressor} instance according to the id and the given properties.
     *
     * @param id         the type of the compression algorithm
     * @param properties a Map containing the compressor specific properties
     * @return a new Compressor instance according to the id and the properties
     * @throws IllegalArgumentException If it is not able to create a Compressor.
     */
    public static Compressor create(String id, Map<String, Object> properties) {
        if ("null".equals(id)) {
            return nullCompressor;
        }
        if ("zlib".equals(id)) {
            return new ZlibCompressor(properties);
        }
        if ("blosc".equals(id)) {
            return new BloscCompressor(properties);
        }
        if ("dtcop".equals(id)) {
        	return new DTCOPCompressor(properties, 1.0, 10);
        }
        throw new IllegalArgumentException("Compressor id:'" + id + "' not supported.");
    }

    private static Map<String, Object> toMap(Object... args) {
        final HashMap<String, Object> map = new HashMap<>();
        for (int i = 0; i < args.length; i += 2) {
            String key = (String) args[i];
            Object val = args[i + 1];
            map.put(key, val);
        }
        return map;
    }

}

