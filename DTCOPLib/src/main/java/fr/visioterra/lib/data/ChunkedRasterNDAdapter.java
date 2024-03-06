package fr.visioterra.lib.data;



public abstract class ChunkedRasterNDAdapter implements ChunkedRasterND {
	
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
		
		int[] shapes = this.getShape();
		int[] chunks = this.getChunkShape();
		
		int[] numChunk = new int[chunks.length];
		
		for (int i = 0; i < numChunk.length; i += 1) {
			int shape = shapes[i];
			int chunk = chunks[i];
			
			numChunk[i] = shape % chunk == 0 ? shape / chunk : shape / chunk + 1;
		}
		
		return numChunk;

	}

	@Override public int coordToChunk(int coord, int dimension) throws IllegalArgumentException {
		return coord / getChunkShape()[dimension];
	}

	@Override public int chunkToCoord(int chunk, int dimension) throws IllegalArgumentException {
		return chunk * getChunkShape()[dimension];
	}

}
