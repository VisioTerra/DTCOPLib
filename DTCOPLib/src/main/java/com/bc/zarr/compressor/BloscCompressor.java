package com.bc.zarr.compressor;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.blosc.BufferSizes;
import org.blosc.IBloscDll;
import org.blosc.JBlosc;

import com.bc.zarr.Compressor;
import com.sun.jna.ptr.NativeLongByReference;


public class BloscCompressor extends Compressor {

	final static int AUTOSHUFFLE = -1;
	final static int NOSHUFFLE = 0;
	final static int BYTESHUFFLE = 1;
	final static int BITSHUFFLE = 2;

	public final static String keyCname = "cname";
	public final static String defaultCname = "lz4";
	public final static String keyClevel = "clevel";
	public final static int defaultCLevel = 5;
	public final static String keyShuffle = "shuffle";
	public final static int defaultShuffle = BYTESHUFFLE;
	public final static String keyBlocksize = "blocksize";
	public final static int defaultBlocksize = 0;
	public final static String keyNumThreads = "nthreads";
	public final static int defaultNumThreads = 1;
	public final static int[] supportedShuffle = new int[]{/*AUTOSHUFFLE, */NOSHUFFLE, BYTESHUFFLE, BITSHUFFLE};
	public final static String[] supportedCnames = new String[]{"zstd", "blosclz", defaultCname, "lz4hc", "zlib"/*, "snappy"*/};

	public final static Map<String, Object> defaultProperties = new HashMap<String, Object>() {{
		put(keyCname, defaultCname);
		put(keyClevel, defaultCLevel);
		put(keyShuffle, defaultShuffle);
		put(keyBlocksize, defaultBlocksize);
		put(keyNumThreads, defaultNumThreads);
	}};

	private final int clevel;
	private final int blocksize;
	private final int shuffle;
	private final String cname;
	private final int nthreads;

	public BloscCompressor(Map<String, Object> map) {
		final Object cnameObj = map.get(keyCname);
		if (cnameObj == null) {
			cname = defaultCname;
		} else {
			cname = (String) cnameObj;
		}
		if (Arrays.stream(supportedCnames).noneMatch(cname::equals)) {
			throw new IllegalArgumentException(
					"blosc: compressor not supported: '" + cname + "'; expected one of " + Arrays.toString(supportedCnames));
		}

		final Object clevelObj = map.get(keyClevel);
		if (clevelObj == null) {
			clevel = defaultCLevel;
		} else if (clevelObj instanceof String) {
			clevel = Integer.parseInt((String) clevelObj);
		} else {
			clevel = ((Number) clevelObj).intValue();
		}
		if (clevel < 0 || clevel > 9) {
			throw new IllegalArgumentException("blosc: clevel parameter must be between 0 and 9 but was: " + clevel);
		}

		final Object shuffleObj = map.get(keyShuffle);
		if (shuffleObj == null) {
			this.shuffle = defaultShuffle;
		} else if (shuffleObj instanceof String) {
			this.shuffle = Integer.parseInt((String) shuffleObj);
		} else {
			this.shuffle = ((Number) shuffleObj).intValue();
		}
		final String[] supportedShuffleNames = new String[]{/*"-1 (AUTOSHUFFLE)", */"0 (NOSHUFFLE)", "1 (BYTESHUFFLE)", "2 (BITSHUFFLE)"};
		if (Arrays.stream(supportedShuffle).noneMatch(value -> value == shuffle)) {
			throw new IllegalArgumentException(
					"blosc: shuffle type not supported: '" + shuffle + "'; expected one of " + Arrays.toString(supportedShuffleNames));
		}

		final Object blocksizeObj = map.get(keyBlocksize);
		if (blocksizeObj == null) {
			this.blocksize = defaultBlocksize;
		} else if (blocksizeObj instanceof String) {
			this.blocksize = Integer.parseInt((String) blocksizeObj);
		} else {
			this.blocksize = ((Number) blocksizeObj).intValue();
		}

		Object nthreadsObj = map.get(keyNumThreads);
		if (nthreadsObj == null) {
			nthreadsObj = defaultProperties.get(keyNumThreads);
		}
		if (nthreadsObj instanceof String) {
			this.nthreads = Integer.parseInt((String) nthreadsObj);
		} else {
			this.nthreads = ((Number) nthreadsObj).intValue();
		}
	}

	@Override
	public String getId() {
		return "blosc";
	}

	public int getClevel() {
		return clevel;
	}

	public int getBlocksize() {
		return blocksize;
	}

	public int getShuffle() {
		return shuffle;
	}

	public String getCname() {
		return cname;
	}

	public int getNumThreads() {
		return nthreads;
	}

	@Override
	public String toString() {
		return "compressor=" + getId()
		+ "/cname=" + cname + "/clevel=" + clevel
		+ "/blocksize=" + blocksize + "/shuffle=" + shuffle;
	}

	@Override public String toShortString() {
		return getId() + "-" + cname + "-" + clevel + "-" + blocksize + "-" + shuffle;
	}

	@Override
	public void compress(InputStream is, OutputStream os) throws IOException {
		final ByteArrayOutputStream baos = new ByteArrayOutputStream();
		passThrough(is, baos);
		final byte[] inputBytes = baos.toByteArray();
		final int inputSize = inputBytes.length;
		final int outputSize = inputSize + JBlosc.OVERHEAD;
		final ByteBuffer inputBuffer = ByteBuffer.wrap(inputBytes);
		final ByteBuffer outBuffer = ByteBuffer.allocate(outputSize);
		final int i = JBlosc.compressCtx(clevel, shuffle, 1, inputBuffer, inputSize, outBuffer, outputSize, cname, blocksize, nthreads);
		final BufferSizes bs = cbufferSizes(outBuffer);
		byte[] compressedChunk = Arrays.copyOfRange(outBuffer.array(), 0, (int) bs.getCbytes());
		os.write(compressedChunk);
	}

	@Override
	public void uncompress(InputStream is, OutputStream os) throws IOException {
		final DataInput di = new DataInputStream(is);
		byte[] header = new byte[JBlosc.OVERHEAD];
		di.readFully(header);
		BufferSizes bs = cbufferSizes(ByteBuffer.wrap(header));
		int compressedSize = (int) bs.getCbytes();
		int uncompressedSize = (int) bs.getNbytes();
		byte[] inBytes = Arrays.copyOf(header, compressedSize);
		di.readFully(inBytes, header.length, compressedSize - header.length);
		ByteBuffer outBuffer = ByteBuffer.allocate(uncompressedSize);
		JBlosc.decompressCtx(ByteBuffer.wrap(inBytes), outBuffer, outBuffer.limit(), nthreads);
		os.write(outBuffer.array());
	}

	private BufferSizes cbufferSizes(ByteBuffer cbuffer) {
		NativeLongByReference nbytes = new NativeLongByReference();
		NativeLongByReference cbytes = new NativeLongByReference();
		NativeLongByReference blocksize = new NativeLongByReference();
		IBloscDll.blosc_cbuffer_sizes(cbuffer, nbytes, cbytes, blocksize);
		BufferSizes bs = new BufferSizes(nbytes.getValue().longValue(),
				cbytes.getValue().longValue(),
				blocksize.getValue().longValue());
		return bs;
	}
}
