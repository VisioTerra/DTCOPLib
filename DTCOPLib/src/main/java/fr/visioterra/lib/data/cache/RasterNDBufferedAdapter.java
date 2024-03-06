package fr.visioterra.lib.data.cache;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.List;

import fr.visioterra.lib.data.ChunkedRasterNDAdapter;
import fr.visioterra.lib.data.ChunkedRasterND;
import fr.visioterra.lib.data.RasterND;
import fr.visioterra.lib.data.RasterNDMetadata;
import ucar.ma2.Array;
import ucar.ma2.Index;
import ucar.ma2.Section;
import ucar.nc2.Dimension;

public abstract class RasterNDBufferedAdapter extends ChunkedRasterNDAdapter implements RasterND, ChunkedRasterND {
	
	protected static class SerializeArray implements Externalizable {
		
		private Array array;
		
		
		public SerializeArray(Array array) {
			this.array = array;
		}

		
		public Array getArray() {
			return array;
		}
		
		@Override public void writeExternal(ObjectOutput oo) throws IOException {
	        if(array!=null) {
	            oo.writeObject(array.getDataType());
	            oo.writeObject(array.getShape());
	            oo.writeObject(array.getStorage());
	        }
		}

		@Override public void readExternal(ObjectInput oi) throws IOException, ClassNotFoundException {
			array = Array.factory((ucar.ma2.DataType) oi.readObject(), (int[]) oi.readObject(), oi.readObject());
		}
		
	}
	
	protected static class ChunkKey {
		
		private final String id;
		private final int[] coordinates;
		private boolean computeHash;
		private int hash;
		
		
		public ChunkKey(String id, int[] coordinates) {
			this.id = id;
			this.coordinates = Arrays.copyOf(coordinates, coordinates.length);
			this.computeHash = true;
		}

		@Override public boolean equals(Object obj) {
			if (this == obj) return true;
			if (obj == null) return false;
			if (getClass() != obj.getClass()) return false;
			ChunkKey other = (ChunkKey) obj;
			if (!Arrays.equals(coordinates, other.coordinates)) return false;
			if (id == null) {
				if (other.id != null) return false;
			}
			else if (!id.equals(other.id)) return false;
			return true;
		}

		@Override public int hashCode() {
			if (computeHash) {
				final int prime = 31;
				int result = 1;
				result = prime * result + Arrays.hashCode(coordinates);
				result = prime * result + ((id == null) ? 0 : id.hashCode());
				this.hash = result;
			}
			
			return this.hash;
		}
		
	}
	
	private static class BufferedChunck {
		
		private int[] coordinates;
		private Array chunk;
		
		public BufferedChunck(int[] coordinates, Array chunk) {
			this.coordinates = coordinates;
			this.chunk = chunk;
		}
		
		public Array getChunk(int[] coordinates) {
			if (Arrays.equals(this.coordinates, coordinates)) {
				return chunk;
			}
			
			return null;
		}
		
		public void setChunk(int[] coordinates, Array chunk) {
			this.coordinates = Arrays.copyOf(coordinates, coordinates.length);
			this.chunk = chunk;
		}
		
	}
	
	
//	private final RasterNDMetadata rasterMetadata;
	private final int[] chunkShape;
	
	private final ThreadLocal<BufferedChunck> threadLocal = new ThreadLocal<>();
	
	
	public RasterNDBufferedAdapter(int[] chunkShape) {
		this.chunkShape = chunkShape;
	}
	
	
	protected abstract RasterNDMetadata getMetadata();
	
	protected abstract Array getBufferedChunk(int[] chunkCoords) throws Exception;
	
	
	private Array getLocalChunk(int[] chunkCoords) throws Exception {
		
		Array chunk = null;
		BufferedChunck bc = threadLocal.get();
		
		if (bc == null) {
			chunk = getBufferedChunk(chunkCoords);
			bc = new BufferedChunck(chunkCoords, chunk);
			threadLocal.set(bc);
		}
		
		else {
			chunk = bc.getChunk(chunkCoords);
			if (chunk == null) {
				chunk = getBufferedChunk(chunkCoords);
				bc.setChunk(chunkCoords, chunk);
			}
		}
		
		return chunk;
	}
	

	@Override public List<Dimension> getDimensions() {
		return this.getMetadata().getDimensions();
	}

	@Override public int[] getShape() {
		return this.getMetadata().getShape();
	}

	@Override public int getLength(int dimension) {
		return this.getMetadata().getLength(dimension);
	}

	@Override public int getLength(String dimension) {
		return this.getMetadata().getLength(dimension);
	}

	@Override public int getDataType() {
		return this.getMetadata().getDataType();
	}

	@Override public double getScaleFactor() {
		return this.getMetadata().getScaleFactor();
	}

	@Override public double getAddOffset() {
		return this.getMetadata().getAddOffset();
	}

	@Override public int[] getChunkShape() {
		return this.chunkShape;
	}

	@Override public boolean hasChunk(int[] chunk) throws IllegalArgumentException, Exception {
		return true;
	}
	
	@Override public Array getChunk(int[] chunk) throws IllegalArgumentException, Exception {
		return getLocalChunk(chunk);
	}

	public boolean hack = false;
	
	
	
	
	
	@Override public ucar.ma2.DataType getArrayDataType() {
		return RasterND.getArrayDataType(getDataType());
	}
	
	private void copy(Array outputArray, Section section, int dim, int[] cIdxCoords) throws Exception {

		int[] outputOrigin = section.getOrigin();
		int[] outputShape  = section.getShape();
		int[] inputShape   = getShape();
		int[] inputChunkShape = getChunkShape();
		
		int cIdxMin = (outputOrigin[dim]                   ) / inputChunkShape[dim];
		int cIdxMax = (outputOrigin[dim] + outputShape[dim]) / inputChunkShape[dim];
		
		//TODO : remove debug
//		StringBuilder sb = new StringBuilder();
//		for(int d = 0 ; d < dim ; d++) {
//			sb.append("  ");
//		}
//		System.out.println(sb.toString() + "dim[" + dim + "] " + cIdxMin + " > " + cIdxMax);
		
		for(int cIdx = cIdxMin ; cIdx <= cIdxMax ; cIdx++) {
			
			cIdxCoords[dim] = cIdx;
			
			if(dim == inputShape.length - 1) {
//				System.out.println(sb.toString() + "getBufferedChunk(" + Arrays.toString(cIdxCoords) + ")");
				Array inputArray = getBufferedChunk(cIdxCoords);
				
				//copy part of inputArray to outputArray
				
			}
			else {
				copy(outputArray,section,dim+1,cIdxCoords);
			}
		}
		
	}
	
	
	private static class ChunkInfo {
		
		private final int chunkIndex;
		private final int inputMin;
		private final int inputMax;
		private final int outputMin;
		private final int outputMax;
		
		public ChunkInfo(int chunkIndex, int inputMin, int inputMax, int outputMin, int outputMax) {
			this.chunkIndex = chunkIndex;
			this.inputMin = inputMin;
			this.inputMax = inputMax;
			this.outputMin = outputMin;
			this.outputMax = outputMax;
		}
		
		public int getChunkIndex() {
			return this.chunkIndex;
		}
		
		public int getInputMin() {
			return this.inputMin;
		}
		
		public int getInputMax() {
			return this.inputMax;
		}
		
		public int getOutputMin() {
			return this.outputMin;
		}
		
		public int getOutputMax() {
			return this.outputMax;
		}
		
		@Override public String toString() {
			return this.getClass().getSimpleName() + "(" +
					this.chunkIndex + "," +
					this.inputMin + "," +
					this.inputMax + "," +
					this.outputMin + "," +
					this.outputMax + ")";
		}
		
	}
	
	@Override public Array getArray(Section section) throws Exception {
		
		//loop on dimensions
		//for each dim, compute min/max chunk index

		int dim            = section.getOrigin().length;
		int[] outputOrigin = section.getOrigin();
		int[] outputShape  = section.getShape();
		
		int[] inputShape   = getShape();
		
		int[] inputChunkShape = getChunkShape();
		
		
		//min/max chunk indices - {min D0, max D0, min D1, max D1, ... }
		int[] chunksIndex = new int[dim * 2];
		
		Array outputArray = Array.factory(getArrayDataType(),outputShape);
		
		copy(outputArray, section, 0, new int[dim]);
		
		
		/*
		StringBuilder sb = new StringBuilder("getArray(" + section.toString() + ")");
		
		for(int d = 0 ; d < dim ; d++) {
			
			int idxMin = (outputOrigin[d]                 ) / inputChunkShape[d]; 
			int idxMax = (outputOrigin[d] + outputShape[d]) / inputChunkShape[d];
			
			chunksIndex[d * 2 + 0] = idxMin;
			chunksIndex[d * 2 + 1] = idxMax;

			
			for(int chunkIndex = idxMin ; chunkIndex <= idxMax ; chunkIndex++) {
				
				int chunkSize = inputChunkShape[d];

				
				int inputMin;
				int inputMax;
				int outputMin;
				int outputMax;
				
				new ChunkInfo(chunkIndex, inputMin, inputMax, outputMin, outputMax);
			}
			
			
			sb.append(" - idx dim[" + d + "] " + idxMin + " > " + idxMax);
		}
		
		System.out.println(sb.toString());
		*/
		
		return outputArray;
	}
	
	public Array getArray1(Section section) throws Exception {
		
		if(this.hack) {
			
			int[] origin = section.getOrigin();
			int[] shape  = section.getShape();
			
			
			//Array array = Array.factory(DataType.SHORT, section.getShape());

			int[] coordinates = section.getOrigin();
			//int[] shape  = section.getShape();

			int[] chunkCoord = new int[coordinates.length];
			int[] chunkOffset = new int[coordinates.length];

			for (int i = 0; i < coordinates.length; i += 1) {
				chunkCoord[i] = coordToChunk(coordinates[i], i);
				chunkOffset[i] = coordinates[i] - chunkToCoord(chunkCoord[i], i);
			}
			//return getLocalChunk(chunkCoord).section(origin, shape);
			
			Array array = getLocalChunk(chunkCoord);
			
//			System.out.println(getClass().getSimpleName() + ".getArray(...) / origin: " + Arrays.toString(origin) + " / shape:" + Arrays.toString(shape));
			
//			System.out.print(getClass().getSimpleName() + ".getArray(...)");
//			System.out.print(" - section - origin: " + Arrays.toString(origin) + " / shape:" + Arrays.toString(shape));
//			System.out.println(" - getLocalChunk(" + Arrays.toString(chunkCoord) + ") / shape:" + Arrays.toString(array.getShape()));
			
			origin[0] = 0;
//			return Array.factory(ucar.ma2.DataType.SHORT, section.getShape());
			return array.section(origin, shape);
			
//			throw new UnsupportedOperationException();
		}
		else {
			throw new UnsupportedOperationException();
		}
		
	}

	@Override public int getSampleInt(int[] coordinates) throws Exception {
		
		if (coordinates.length != this.getDimensions().size()) throw new IllegalArgumentException();
		
		int[] chunkCoord = new int[coordinates.length];
		int[] chunkOffset = new int[coordinates.length];
		
		for (int i = 0; i < coordinates.length; i += 1) {
			chunkCoord[i] = coordToChunk(coordinates[i], i);
			chunkOffset[i] = coordinates[i] - chunkToCoord(chunkCoord[i], i);
		}
		
		Array array = getLocalChunk(chunkCoord);
		
		Index index = array.getIndex();
		index.set(chunkOffset);
		return array.getInt(index);
	}

	@Override public long getSampleLong(int[] coordinates) throws Exception {
		
		if (coordinates.length != this.getDimensions().size()) throw new IllegalArgumentException();
		
		int[] chunkCoord = new int[coordinates.length];
		int[] chunkOffset = new int[coordinates.length];
		
		for (int i = 0; i < coordinates.length; i += 1) {
			chunkCoord[i] = coordToChunk(coordinates[i], i);
			chunkOffset[i] = coordinates[i] - chunkToCoord(chunkCoord[i], i);
		}
		
		Array array = getLocalChunk(chunkCoord);
		
		Index index = array.getIndex();
		index.set(chunkOffset);
		return array.getLong(index);
	}

	@Override public float getSampleFloat(int[] coordinates) throws Exception {
		
		if (coordinates.length != this.getDimensions().size()) throw new IllegalArgumentException();
		
		int[] chunkCoord = new int[coordinates.length];
		int[] chunkOffset = new int[coordinates.length];
		
		for (int i = 0; i < coordinates.length; i += 1) {
			chunkCoord[i] = coordToChunk(coordinates[i], i);
			chunkOffset[i] = coordinates[i] - chunkToCoord(chunkCoord[i], i);
		}
		
		Array array = getLocalChunk(chunkCoord);
		
		Index index = array.getIndex();
		index.set(chunkOffset);
		return array.getFloat(index);
	}

	@Override public double getSampleDouble(int[] coordinates) throws Exception {
		
		if (coordinates.length != this.getDimensions().size()) throw new IllegalArgumentException();
		
		int[] chunkCoord = new int[coordinates.length];
		int[] chunkOffset = new int[coordinates.length];
		
		for (int i = 0; i < coordinates.length; i += 1) {
			chunkCoord[i] = coordToChunk(coordinates[i], i);
			chunkOffset[i] = coordinates[i] - chunkToCoord(chunkCoord[i], i);
		}
		
		Array array = getLocalChunk(chunkCoord);
		
		Index index = array.getIndex();
		index.set(chunkOffset);
		return array.getDouble(index);
	}

	@Override public int getSampleInt(Index index) throws Exception {
		return getSampleInt(index.getCurrentCounter());
	}

	@Override public long getSampleLong(Index index) throws Exception {
		return getSampleLong(index.getCurrentCounter());
	}

	@Override public float getSampleFloat(Index index) throws Exception {
		return getSampleFloat(index.getCurrentCounter());
	}

	@Override public double getSampleDouble(Index index) throws Exception {
		return getSampleDouble(index.getCurrentCounter());
	}

}
