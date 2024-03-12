package fr.visioterra.lib.image.raster2D;


public interface Raster2DMetadata {
	
	/**
	 * @return width of the raster
	 */
	public int getWidth();
	
	/**
	 * @return height of the raster
	 */
	public int getHeight();
	
	/**
	 * @return the band number of the raster
	 */
	public int getBandNumber();
	
	/**
	 * @return true if pixel organization is Band-Interleaved-by-Pixel
	 * (also named Pixel-Interleaved) and false if pixel organization is
	 * Band-Sequential (also named Band-Interleaved).
	 */
	public boolean isBIP();
	
	/**
	 * @return DataType of the data
	 */
	public int getDataType();
	
	
	
}
