package fr.visioterra.lib.data;

import ucar.ma2.Array;

public interface ChunkedRasterND extends RasterNDMetadata, AutoCloseable {

	public int[] getChunkShape();
	
	public int[] getChunkShape(int[] chunk) throws IllegalArgumentException;
	
	public int[] getNumChunk();
	
	public int[] coordToChunk(int[] coord) throws IllegalArgumentException;
	
	public int coordToChunk(int coord, int dimension) throws IllegalArgumentException;
	
	public int chunkToCoord(int chunk, int dimension) throws IllegalArgumentException;
	
	public boolean hasChunk(int[] chunk) throws IllegalArgumentException, Exception;
	
	public Array getChunk(int[] chunk) throws IllegalArgumentException, Exception;
	
	@Override public void close() throws Exception; 
	
}
