package fr.visioterra.lib.image.raster2D;

/**
 * @TiledRaster2D interface.
 * This interface describe a raster as a regular array of tiles, except the last row and column of tiles which depend on raster dimension.
 * @author Grégory Mazabraud
 * 
 * <br>Changes :
 * <br>- 2013/11/26		|	Grégory Mazabraud		|	initial version
 * <br>- 2014/02/28		|	Grégory Mazabraud		|	remove setTileWidth/Height/Dimension methods
 */
public interface TiledRaster2D extends Raster2DMetadata, AutoCloseable {

	/*
	voir API :
	- http://docs.oracle.com/cd/E17802_01/products/products/java-media/jai/forDevelopers/jai-apidocs/javax/media/jai/PlanarImage.html
	- http://docs.oracle.com/javase/6/docs/api/java/awt/image/RenderedImage.html
	- http://docs.oracle.com/cd/E17802_01/products/products/java-media/jai/forDevelopers/jai-apidocs/javax/media/jai/TiledImage.html
	*/
	
	
	public int getTileWidth();

	public int getTileHeight();
	
	public int getTileWidth(int tileX) throws IllegalArgumentException;
	
	public int getTileHeight(int tileY) throws IllegalArgumentException;
	
	public int getNumXTiles();
	
	public int getNumYTiles();
	
	public int XToTileX(int x) throws IllegalArgumentException;
	
	public int YToTileY(int y) throws IllegalArgumentException;
	
	public int tileXToX(int tileX) throws IllegalArgumentException;
	
	public int tileYToY(int tileY) throws IllegalArgumentException;
	
	public boolean hasTile(int tileX, int tileY) throws IllegalArgumentException, Exception;

	public Raster2D getTile(int tileX, int tileY) throws IllegalArgumentException, Exception;

	public void close() throws Exception;
	
}
