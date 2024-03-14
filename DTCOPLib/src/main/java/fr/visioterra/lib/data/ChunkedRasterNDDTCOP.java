package fr.visioterra.lib.data;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import com.bc.zarr.Compressor;
import com.bc.zarr.ZarrArray;
import com.bc.zarr.ZarrGroup;

import fr.visioterra.lib.cache.KVCacheMap;
import fr.visioterra.lib.cache.KVStore;
import fr.visioterra.lib.cache.KVStore.KVStoreEntry;
import fr.visioterra.lib.cache.KVStore.KVStoreEventListener;
import fr.visioterra.lib.format.dtcop.shard.ShardReader;
import fr.visioterra.lib.format.dtcop.zarr.QuantCompressor;
import fr.visioterra.lib.format.dtcop.zarr.ZarrFilter;
import fr.visioterra.lib.format.dtcop.zarr.ZarrReader;
import fr.visioterra.lib.image.dataBuffer.DataType;
import fr.visioterra.lib.tools.Benchmark;
import ucar.ma2.Array;
import ucar.nc2.Dimension;

public class ChunkedRasterNDDTCOP implements ChunkedRasterND {
	
	
	private static double getAttributeAsDouble(ZarrArray array, String attributeName, double defaultValue) throws IOException {
		Object attribute = array.getAttributes().get(attributeName);
		
		if (attribute == null) {
			return defaultValue;
		}
		
		if (attribute instanceof Number) {
			return ((Number) attribute).doubleValue();
		}
		
		return defaultValue;
	}
	
	private static int getDataType(com.bc.zarr.DataType type) throws IllegalArgumentException {

		switch (type) {

			case u1: {
				return DataType.TYPE_UINT8;
			}

			case i1: {
				return DataType.TYPE_INT8;
			}

			case u2: {
				return DataType.TYPE_UINT16;
			}
			
			case i2: {
				return DataType.TYPE_INT16;
			}
			
			case u4: {
				return DataType.TYPE_INT32;
			}

			case i4: {
				return DataType.TYPE_INT32;
			}

			case i8: {
				return DataType.TYPE_INT64;
			}

			case f4: {
				return DataType.TYPE_FLOAT;
			}

			case f8: {
				return DataType.TYPE_DOUBLE;
			}

			default: {
				throw new IllegalArgumentException("Unsupported dataBufferType : " + type);
			}
		}
	}

	
	private static final KVCacheMap<String,ShardReader> shardCache = new KVCacheMap<>(true,10000);
	private static final Object lock = new Object();
	
	
	static {
		
		shardCache.addKVStoreListener(new KVStoreEventListener<String, ShardReader>() {
			
			private void close(KVStoreEntry<ShardReader> entry) {
				try {
					entry.getValue().close();
				} catch(Exception e) {
					e.printStackTrace();
				}
			}
			
			@Override public void notifyRemoveAll(KVStore<String, ShardReader> kvStore) {
				try {
					for (String key : kvStore.keys()) {
						close(kvStore.getEntry(key));
					}
				}
				catch (Exception e) {
				}
			}
			
			@Override public void notifyEntryUpdated(KVStore<String, ShardReader> kvStore, String key, KVStoreEntry<ShardReader> entry) {
				//nothing to do
			}
			
			@Override public void notifyEntryRemoved(KVStore<String, ShardReader> kvStore, String key, KVStoreEntry<ShardReader> entry) {
				close(entry);
			}
			
			@Override public void notifyEntryPut(KVStore<String, ShardReader> kvStore, String key, KVStoreEntry<ShardReader> entry) {
				//nothing to do
			}
			
			@Override public void notifyEntryEvicted(KVStore<String, ShardReader> kvStore, String key, KVStoreEntry<ShardReader> entry) {
				close(entry);
			}
		});
		
	}
	
	
	private final ZarrArray dataArray;
	private final List<Dimension> dimensions;

	private final int dataType;
	private final double dataScaleFactor;
	private final double dataAddOffset;
	
	private final ZarrFilter filter;
	
	private final int[] shape;
	private final int[] shardShape;
	private final int[] chunkShape;
	private final int[] numChunk;
	
	
	
	public ChunkedRasterNDDTCOP(File rootDir, String dataVarName) throws IOException {
		this(rootDir,dataVarName,null);
	}
	
	public ChunkedRasterNDDTCOP(File rootDir, String dataVarName, ZarrFilter filter) throws IOException {
		this(ZarrGroup.open(rootDir.getAbsolutePath()),dataVarName,filter);
		
//		ZarrGroup zarrGroup = ZarrGroup.open(rootDir.getAbsolutePath());
//		
//		Set<String> arrayKeys = zarrGroup.getArrayKeys();
//		
//		if (arrayKeys.contains(dataVarName) == false) {
//			throw new IllegalArgumentException("Invalid var name (" + dataVarName + ")");
//		}
//		
//		this.dataArray = zarrGroup.openArray(dataVarName);
//
//		this.dataScaleFactor = getAttributeAsDouble(dataArray, "scale_factor", Double.NaN);
//		this.dataAddOffset   = getAttributeAsDouble(dataArray, "add_offset", Double.NaN);
//		
//		this.dataType = getDataType(dataArray.getDataType());
//		
//		
//		List<String> dimensionNames = ZarrReader.getArrayDimensions(dataArray);
//		int[] shape = dataArray.getShape();
//		
//		if (dimensionNames.size() != shape.length) {
//			throw new IllegalArgumentException("Incoherent dimension names and shapes");
//		}
//		
//		this.dimensions = new ArrayList<>();
//		int index = 0;
//		for (String dimensionName : dimensionNames) {
//			dimensions.add(new Dimension(dimensionName, shape[index]));
//			index += 1;
//		}
//		
//		this.filter = filter;
	}
	
	public ChunkedRasterNDDTCOP(ZarrGroup zarrGroup, String dataVarName, ZarrFilter filter) throws IOException {
		this(zarrGroup,dataVarName,filter,null);
	}
		
	public ChunkedRasterNDDTCOP(ZarrGroup zarrGroup, String dataVarName, ZarrFilter filter, int[] forceChunkShape) throws IOException {
		
		Set<String> arrayKeys = zarrGroup.getArrayKeys();
		
		if (arrayKeys.contains(dataVarName) == false) {
			throw new IllegalArgumentException("Invalid var name (" + dataVarName + ")");
		}
		
		this.dataArray = zarrGroup.openArray(dataVarName);

		this.dataScaleFactor = getAttributeAsDouble(dataArray, "scale_factor", Double.NaN);
		this.dataAddOffset   = getAttributeAsDouble(dataArray, "add_offset", Double.NaN);
		
		//TODO : remove debug
//		System.out.println(this.dataScaleFactor + " / " + this.dataAddOffset);
		
		this.dataType = getDataType(dataArray.getDataType());
		
		
		List<String> dimensionNames = ZarrReader.getArrayDimensions(dataArray);
		this.shape = dataArray.getShape();
		
		if (dimensionNames.size() != this.shape.length) {
			throw new IllegalArgumentException("Incoherent dimension names and shapes");
		}
		
		this.dimensions = new ArrayList<>();
		int index = 0;
		for (String dimensionName : dimensionNames) {
			dimensions.add(new Dimension(dimensionName, this.shape[index]));
			index += 1;
		}
		
		this.filter = filter;
		
		dataArray.getCompressor();
		
		if(forceChunkShape != null) {
			this.chunkShape = forceChunkShape;
			this.shardShape = this.dataArray.getChunks();
		}
		else {
			this.chunkShape = this.dataArray.getChunks();
			this.shardShape = null;
		}
		
		{
			this.numChunk = new int[this.shape.length];
			for (int i = 0; i < numChunk.length; i += 1) {
				int shape = this.shape[i];
				int chunk = this.chunkShape[i];
				this.numChunk[i] = shape % chunk == 0 ? shape / chunk : shape / chunk + 1;
			}
		}
		
	}


	@Override public List<Dimension> getDimensions() {
		return new ArrayList<Dimension>(this.dimensions);
	}

	@Override public int[] getShape() {
		return this.shape;
	}

	@Override public int getLength(int dimension) {
		return this.getShape()[dimension];
	}

	@Override public int getLength(String dimension) {
		Objects.requireNonNull(dimension);
		
		for (Dimension dim : this.dimensions) {
			if (dimension.equalsIgnoreCase(dim.getName())) {
				return dim.getLength();
			}
		}
		
		return -1;
	}

	@Override public int getDataType() {
		return this.dataType;
	}

	@Override public double getScaleFactor() {
		return this.dataScaleFactor;
	}

	@Override public double getAddOffset() {
		return this.dataAddOffset;
	}

	public int[] getShardShape() {
		return this.shardShape;
	}
	
	@Override public int[] getChunkShape() {
//		return this.dataArray.getChunks();
		return this.chunkShape;
	}

	@Override public int[] getChunkShape(int[] chunk) throws IllegalArgumentException {
		
		int[] shapes = this.getShape();
		
		if (chunk.length != shapes.length) {
			throw new IllegalArgumentException("Invalid chunk coordinates (Not enough dimensions)");
		}
		
		int[] numChunks = getNumChunk();
		int[] nominalChunkShape = getChunkShape();
		
		int[] chunkShape = new int[numChunks.length];
		
		for (int i = 0; i < chunkShape.length; i += 1) {
			chunkShape[i] = chunk[i] == (numChunks[i]-1) ? shapes[i] - (numChunks[i]-1) * nominalChunkShape[i] : nominalChunkShape[i];
		}
		
		return chunkShape;
	}

	@Override public int[] getNumChunk() {
		
		return this.numChunk;
		
//		int[] shapes = this.getShape();
//		int[] chunks = this.getChunkShape();
//		
//		int[] numChunk = new int[chunks.length];
//		
//		for (int i = 0; i < numChunk.length; i += 1) {
//			int shape = shapes[i];
//			int chunk = chunks[i];
//			
//			numChunk[i] = shape % chunk == 0 ? shape / chunk : shape / chunk + 1;
//		}
//		
//		return numChunk;
	}

	@Override public int[] coordToChunk(int[] coord) throws IllegalArgumentException {
		int[] chunkShape = getChunkShape();
		int[] chunk = new int[chunkShape.length];
		for (int dim = 0; dim < chunk.length; dim += 1) {
			chunk[dim] = coord[dim] / chunkShape[dim];
		}
		return chunk;
	}
	
	@Override public int coordToChunk(int coord, int dimension) throws IllegalArgumentException {
		return coord / getChunkShape()[dimension];
	}

	@Override public int chunkToCoord(int chunk, int dimension) throws IllegalArgumentException {
		return chunk * getChunkShape()[dimension];
	}

	@Override public boolean hasChunk(int[] chunkIndex) throws IllegalArgumentException, Exception {
//		dataArray.read(shape)
		return true;
	}

	@Override public Array getChunk(int[] chunkIndex) throws IllegalArgumentException, Exception {
		
		int[] shardIndex = new int[this.dimensions.size()];
		int[] shardChunkIndex = new int[shardIndex.length];
		
		for (int i = 0; i < shardIndex.length; i += 1) {
			
			if(chunkIndex[i] < 0 || getNumChunk()[i] <= chunkIndex[i]) {
				throw new IllegalArgumentException("Invalid chunk index (" + Arrays.toString(chunkIndex) + " / " + Arrays.toString(getNumChunk()));
			}
			
			shardIndex[i] = chunkIndex[i] * getChunkShape()[i] / getShardShape()[i];
			
			//chunkIndex      => 273
			//shardIndex      =>  22 (273 * 32 / 384 = 22.75 => 22)
			//shardChunkIndex => 273 - 22 * (384 / 32) 
			shardChunkIndex[i] = chunkIndex[i] - shardIndex[i] * getShardShape()[i] / getChunkShape()[i];
		}
		
		String shardPath = this.dataArray.getShardPath(shardIndex);
		
		//TODO : remove debug
//		System.out.println(Arrays.toString(chunkIndex));
//		System.out.println(Arrays.toString(shardIndex));
//		System.out.println(Arrays.toString(shardChunkIndex));
//		System.out.println(shardPath);

		
		
		
		
		
		ShardReader sr = shardCache.get(shardPath);
		
		if(sr == null) {
			synchronized (lock) {
				sr = shardCache.get(shardPath);
				if(sr == null) {
					sr = new ShardReader(new File(shardPath));
					shardCache.put(shardPath,sr);
				}
			}
		}
		
		
		return sr.getChunk(shardChunkIndex).getArray();
//		return sr.getChunk(shardChunkIndex, this.dataScaleFactor, this.dataAddOffset).getArray();
	}

	@Override public void close() throws Exception {
		
	}

	
	public Benchmark[] getBenchmarks() {
		
		Compressor compressor = this.dataArray.getCompressor();
		
		if(compressor instanceof QuantCompressor == false) {
			return null;
		}
		else {
			QuantCompressor qc = (QuantCompressor)compressor;
			return new Benchmark[] {qc.bench1,qc.bench2,qc.benchZ,qc.bench3};
		}
			
	}

}
