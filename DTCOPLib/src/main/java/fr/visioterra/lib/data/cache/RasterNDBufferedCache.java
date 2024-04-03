package fr.visioterra.lib.data.cache;

import java.util.Arrays;

import fr.visioterra.lib.cache.KVCacheMap;
import fr.visioterra.lib.cache.KVStore;
import fr.visioterra.lib.data.RasterND;
import fr.visioterra.lib.data.RasterNDMetadata;
import fr.visioterra.lib.image.dataBuffer.DataType;
import ucar.ma2.Array;

public class RasterNDBufferedCache extends RasterNDBufferedAdapter {

	private final RasterND raster;
	
	private final KVStore<Object, Object> cache;
	private final String id;
	private final Object lock = new Object();
	
	public RasterNDBufferedCache(RasterND raster, int[] chunkShape, long maxBytesLocalHeap) {
		super(chunkShape);
		
		this.raster = raster;
		
		int dataTypeSize = DataType.getSize(raster.getDataType());
		
		if (dataTypeSize <= 0) throw new IllegalArgumentException("Invalid raster data type (data type size <= 0)");
		
		long chunkSize = dataTypeSize;
		for (int i = 0; i < chunkShape.length; i += 1) {
			chunkSize *= chunkShape[i];
		}
		chunkSize /= 8; // size in bytes
		
		int cacheSize = (int) (maxBytesLocalHeap / chunkSize);
		
		if (cacheSize < 2) {
			throw new IllegalArgumentException("maxBytesLocalHeap is too small (cacheSize=" + cacheSize + " / maxBytesLocalHeap=" + maxBytesLocalHeap + " / chunkSize=" + chunkSize + ")");
		}
		
//		System.out.println("RasterNDBufferedCache: cacheSize=" + cacheSize);
		
		this.cache = new KVCacheMap<Object, Object>(false, cacheSize);
		this.id = getClass().getSimpleName();
	}
	
	public RasterNDBufferedCache(RasterND raster, int[] chunkShape, KVStore<Object, Object> cache, String id) {
		super(chunkShape);
		
		this.raster = raster;
		this.cache = cache;
		this.id = id;
	}

	@Override protected RasterNDMetadata getMetadata() {
		return this.raster;
	}

	@Override protected Array getBufferedChunk(int[] chunkCoords) throws Exception {
//		System.out.println("getBufferedChunk(" + Arrays.toString(chunkCoords) + ")");
		ChunkKey ck = new ChunkKey(this.id, chunkCoords);

		SerializeArray sChunk = (SerializeArray) this.cache.get(ck);

		if (sChunk == null) {

			synchronized (lock) {
				sChunk = (SerializeArray) this.cache.get(ck);

				if (sChunk == null) {
					
					int[] origin = new int[getDimensions().size()];
					for (int i = 0; i < origin.length; i += 1) {
						origin[i] = chunkToCoord(chunkCoords[i], i);
					}
					int[] shape = getChunkShape(chunkCoords);
					
					Array chunk = this.raster.getArray(origin, shape);

					sChunk = new SerializeArray(chunk);

					this.cache.put(ck, sChunk);
				}
			}
		}

		return sChunk.getArray();

	}
	
	@Override public void close() throws Exception {
		this.raster.close();
	}

}
