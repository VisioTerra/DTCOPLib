package fr.visioterra.lib.data.cache;

import fr.visioterra.lib.cache.KVCacheMap;
import fr.visioterra.lib.cache.KVStore;
import fr.visioterra.lib.data.ChunkedRasterND;
import fr.visioterra.lib.data.RasterNDMetadata;
import fr.visioterra.lib.image.dataBuffer.DataType;
import ucar.ma2.Array;
import ucar.ma2.Section;

public class ChunkedRasterNDBufferedCache extends RasterNDBufferedAdapter {
	
	
	private final ChunkedRasterND chunkedRaster;
	
	private final KVStore<Object, Object> cache;
	private final String id;
	private final Object lock = new Object();

//	public final Benchmark bench1 = new Benchmark("getBufferedChunk");
//	public final Benchmark bench2 = new Benchmark("getChunk");
	
	
	public ChunkedRasterNDBufferedCache(ChunkedRasterND chunkedRaster, long maxBytesLocalHeap) {
		super(chunkedRaster.getChunkShape());
		
		this.chunkedRaster = chunkedRaster;
		
		int dataTypeSize = DataType.getSize(chunkedRaster.getDataType());
		int[] chunkShape = chunkedRaster.getChunkShape();
		
		if (dataTypeSize <= 0) throw new IllegalArgumentException("Invalid raster data type (data type size <= 0)");
		
		long chunkSize = dataTypeSize;
		for (int i = 0; i < chunkShape.length; i += 1) {
			chunkSize *= chunkShape[i];
		}
		chunkSize /= 8; // size in bytes
		
		int cacheSize = (int) (maxBytesLocalHeap / chunkSize);
		
		if (cacheSize < 2) 
			throw new IllegalArgumentException("maxBytesLocalHeap is too small");
		
//		System.out.println("RasterNDBufferedCache: cacheSize=" + cacheSize);
		
		this.cache = new KVCacheMap<Object, Object>(false, cacheSize);
		this.id = getClass().getSimpleName();
	}	
	
	public ChunkedRasterNDBufferedCache(ChunkedRasterND chunkedRaster, KVStore<Object, Object> cache, String id) {
		super(chunkedRaster.getChunkShape());
		
		this.chunkedRaster = chunkedRaster;
		this.cache = cache;
		this.id = id;
	}


	@Override protected RasterNDMetadata getMetadata() {
		return this.chunkedRaster;
	}

	@Override public Array getArray(int[] origin, int[] shape) throws Exception {
		return getArray(new Section(origin, shape));
	}
	
	@Override protected Array getBufferedChunk(int[] chunkCoords) throws Exception {

//		this.bench1.start();
		
		ChunkKey ck = new ChunkKey(this.id, chunkCoords);

		SerializeArray sChunk = (SerializeArray) this.cache.get(ck);

		if (sChunk == null) {

			synchronized (lock) {
				sChunk = (SerializeArray) this.cache.get(ck);

				if (sChunk == null) {
					
//					this.bench2.start();
					
					Array chunk = this.chunkedRaster.getChunk(chunkCoords);
//					System.out.println("getBufferedChunk(" + Arrays.toString(chunkCoords) + ")");
					
//					this.bench2.stop();

					sChunk = new SerializeArray(chunk);

					this.cache.put(ck, sChunk);
				}
			}
		}

//		return sChunk.getArray();
		
		
		Array output = sChunk.getArray();
		
//		this.bench1.stop();

		return output;
		
	}

	@Override public boolean hasChunk(int[] chunk) throws IllegalArgumentException, Exception {
		return this.chunkedRaster.hasChunk(chunk);
	}
	
	@Override public void close() throws Exception {
		this.chunkedRaster.close();
	}

}
