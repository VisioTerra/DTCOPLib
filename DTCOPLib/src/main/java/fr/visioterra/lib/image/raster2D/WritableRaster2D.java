package fr.visioterra.lib.image.raster2D;


/**
 * @WritableRaster2D interface.
 * @author Grégory Mazabraud
 * 
 * <br>Changes :
 * <br>- 2013/11/20		|	Grégory Mazabraud		|	initial version
 * <br>- 2014/10/13		|	Grégory Mazabraud		|	rename setSample/setPixel to setSampleObject/setPixelObject to avoid misunderstanding
 */
public interface WritableRaster2D extends Raster2D {

	public void setSampleInt(int i, int j, int b, int s) throws Exception;
	
	public void setSampleLong(int i, int j, int b, long s) throws Exception;
	
	public void setSampleFloat(int i, int j, int b, float s) throws Exception;
	
	public void setSampleDouble(int i, int j, int b, double s) throws Exception;
	
	public void setSampleObject(int i, int j, int b, Object o) throws Exception;
	
	public void setSamples(int i, int j, int b, Raster2D raster, int rasterBand) throws Exception;
	
	
	public void setPixelInt(int i, int j, int[] iArray) throws Exception;
	
	public void setPixelLong(int i, int j, long[] lArray) throws Exception;
	
	public void setPixelFloat(int i, int j, float[] fArray) throws Exception;
	
	public void setPixelDouble(int i, int j, double[] dArray) throws Exception;
	
	public void setPixelObject(int i, int j, Object[] oArray) throws Exception;
	
	public void setPixels(int i, int j, Raster2D raster) throws Exception;
	
}
