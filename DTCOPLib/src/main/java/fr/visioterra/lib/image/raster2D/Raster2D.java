package fr.visioterra.lib.image.raster2D;

/**
 * @Raster2D interface.
 * @author Grégory Mazabraud
 * 
 * <br>Changes :
 * <br>- 2013/11/18		|	Grégory Mazabraud		|	initial version
 * <br>- 2014/10/13		|	Grégory Mazabraud		|	rename getSample/getPixel to getSampleObject/getPixelObject to avoid misunderstanding
 * 
 * 
 */
public interface Raster2D extends Raster2DMetadata, AutoCloseable {
	
	
	public Raster2D getBand(int band) throws UnsupportedOperationException, Exception;
	
	
	//properties
	/*
	public String getName();
	
	public ImageTypeSpecifier getImageTypeSpecifier();
	
	public String getBandName(int band);
	
	public String getBandUnit(int band);
	
	public int getCyclicWidth();
	
	public int getCyclicHeight();
	
	//dataMissingValue

	//dataShift / dataScaleFactor or DigitalNumberToPhysicalValue ?
	
	//lookupTable
	
	//histogram
	
	//RasterCRS
	
	//toReflectance
	
	//
	
	
	
	
	*/
	
	
	
	/**
	 * @param i horizontal coordinate of the pixel location
	 * @param j vertical coordinate of the pixel location
	 * @param b band index of the pixel
	 * @return the sample value of the specified pixel as an integer 
	 * @throws Exception if an error occurs
	 */
	public int getSampleInt(int i, int j, int b) throws Exception;

	/**
	 * @param i horizontal coordinate of the pixel location
	 * @param j vertical coordinate of the pixel location
	 * @param b band index of the pixel
	 * @return the sample value of the specified pixel as a long 
	 * @throws Exception if an error occurs
	 */
	public long getSampleLong(int i, int j, int b) throws Exception;

	/**
	 * @param i horizontal coordinate of the pixel location
	 * @param j vertical coordinate of the pixel location
	 * @param b band index of the pixel
	 * @return the sample value of the specified pixel as a float 
	 * @throws Exception if an error occurs
	 */
	public float getSampleFloat(int i, int j, int b) throws Exception;

	/**
	 * @param i horizontal coordinate of the pixel location
	 * @param j vertical coordinate of the pixel location
	 * @param b band index of the pixel
	 * @return the sample value of the specified pixel as a double 
	 * @throws Exception if an error occurs
	 */
	public double getSampleDouble(int i, int j, int b) throws Exception;

	public Object getSampleObject(int i, int j, int b, Object o) throws Exception;
	
	public Raster2D getSamples(int i, int j, int w, int h, int b) throws Exception;
	
	
    /**
     * @param i The horizontal coordinate of the pixel location
     * @param j The vertical coordinate of the pixel location
     * @param iArray An optionally preallocated int array
     * @return the samples for the specified pixel, as an int array
     * @throws Exception if an error occurs
     */
	public int[] getPixelInt(int i, int j, int[] iArray) throws Exception;
	
	/**
     * @param i The horizontal coordinate of the pixel location
     * @param j The vertical coordinate of the pixel location
     * @param lArray An optionally preallocated long array
     * @return the samples for the specified pixel, as a long array
     * @throws Exception if an error occurs
     */
	public long[] getPixelLong(int i, int j, long[] lArray) throws Exception;

	/**
     * @param i The horizontal coordinate of the pixel location
     * @param j The vertical coordinate of the pixel location
     * @param fArray An optionally preallocated float array
     * @return the samples for the specified pixel, as a float array
     * @throws Exception if an error occurs
     */
	public float[] getPixelFloat(int i, int j, float[] fArray) throws Exception;

	/**
     * @param i The horizontal coordinate of the pixel location
     * @param j The vertical coordinate of the pixel location
     * @param dArray An optionally preallocated double array
     * @return the samples for the specified pixel, as a double array
     * @throws Exception if an error occurs
     */
	public double[] getPixelDouble(int i, int j, double[] dArray) throws Exception;
	
	/**
     * @param i The horizontal coordinate of the pixel location
     * @param j The vertical coordinate of the pixel location
     * @param pixel An optionally preallocated Pixel
     * @return the samples for the specified pixel, as a Pixel
     * @throws Exception if an error occurs
     */
	public Object[] getPixelObject(int i, int j, Object[] oArray) throws Exception;
	
	/**
	 * Returns a Tile2D containing all samples for a rectangle of pixels
	 * @param i horizontal coordinate of the upper-left pixel location
	 * @param j vertical coordinate of the upper-left pixel location
	 * @param w width of the pixel rectangle
	 * @param h height of the pixel rectangle
	 * @return the samples for the specified rectangle of pixels
	 * @throws Exception if an error occurs
	 */
	public Raster2D getPixels(int i, int j, int w, int h) throws Exception;

	
	/**
	 * Close all resources associated with this Raster2D.
	 * @throws Exception if an error occurs.
	 */
	@Override public void close() throws Exception;
	

}
