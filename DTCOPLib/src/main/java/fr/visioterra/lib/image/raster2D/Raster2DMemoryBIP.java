package fr.visioterra.lib.image.raster2D;

import java.io.Serializable;

import fr.visioterra.lib.image.dataBuffer.DataBuffer;
import fr.visioterra.lib.image.dataBuffer.DataBufferFactory;
import fr.visioterra.lib.image.dataBuffer.DataBufferFactory.DataBufferRawCopy;
import fr.visioterra.lib.image.dataBuffer.DataType;

/**
 * @Raster2DMemoryBIP class.
 * <br>Implementation of @Raster2D / @WritableRaster2D in memory. This class is @Serializable.
 * <br>Pixels are stored with "Pixel Interleaved" pattern so :
 * <br>- Pixel extraction is easy
 * <br>- Band extraction is complex
 * <br>If band number is equal to 1, this class is similar to @Raster2DMemoryBSQ.
 * @author Grégory Mazabraud
 * 
 * <br>Changes :
 * <br>- 2013/11/25		|	Grégory Mazabraud		|	initial version
 * <br>- 2013/12/04		|	Grégory Mazabraud		|	change implements Raster2D to Raster2DMemory, update serialVersionUID
 * <br>- 2013/12/10		|	Grégory Mazabraud		|	rewrite getSamples and getPixels methods
 */
public class Raster2DMemoryBIP implements Raster2DMemory, WritableRaster2D, Serializable {
	
	private static final long serialVersionUID = 310307095488872658L;
	
	private final int width;
	private final int height;
	private final int bandNumber;
	private final DataBuffer bands;


	private static int getIndex(int width, int bandNumber, int i, int j, int b) {
		return ((j * width) + i) * bandNumber + b;
	}
	
	public int getIndex(int i, int j, int b) {
		return ((j * this.width) + i) * this.bandNumber + b;
	}
	
	public DataBuffer getDataBuffer() {
		return this.bands;
	}
	
	public Raster2DMemoryBIP(int width, int height, int bandNumber, DataBuffer bands) throws IllegalArgumentException {
		
		if(bands == null)
			throw new IllegalArgumentException("DataBuffer is null pointer");
		
		if(width * height * bandNumber > bands.size())
			throw new IllegalArgumentException("DataBuffer size is smaller than expected size");
		
		this.width = width;
		this.height = height;
		this.bandNumber = bandNumber;
		this.bands = bands;
	}
	
	public Raster2DMemoryBIP(int width, int height, int bandNumber, int dataType) throws IllegalArgumentException {
		this.width = width;
		this.height = height;
		this.bandNumber = bandNumber;
		this.bands = DataBufferFactory.getDataBuffer(dataType, width * height * bandNumber);
	}
	
	@Override public int getWidth() {
		return this.width;
	}
	
	@Override public int getHeight() {
		return this.height;
	}
	
	@Override public int getBandNumber() {
		return this.bandNumber;
	}
	
	@Override public boolean isBIP() {
		return true;
	}
	
	@Override public int getDataType() {
		return this.bands.getDataType();
	}
	
	@Override public Raster2D getBand(int band) throws UnsupportedOperationException, Exception {
		return getSamples(0, 0, getWidth(), getHeight(), band);
	}
	
	@Override public int getSampleInt(int i, int j, int b) {
		return this.bands.getInt(getIndex(i,j,b));
	}

	@Override public long getSampleLong(int i, int j, int b) {
		return this.bands.getLong(getIndex(i,j,b));
	}

	@Override public float getSampleFloat(int i, int j, int b) {
		return this.bands.getFloat(getIndex(i,j,b));
	}

	@Override public double getSampleDouble(int i, int j, int b) {
		return this.bands.getDouble(getIndex(i,j,b));
	}

	@Override public Object getSampleObject(int i, int j, int b, Object o) {
		return this.bands.getObject(getIndex(i,j,b));
	}
	
	@Override public Raster2D getSamples(int i, int j, int w, int h, int b) {
		
		DataBufferRawCopy rawCopy = DataBufferFactory.rawCopy(this.bands, w * h);

		for(int y = 0 ; y < h ; y++) {
			for(int x = 0 ; x < w ; x++) {
				rawCopy.copy(getIndex(i+x, j+y, b), y * w + x);
			}
		}
		
		return new Raster2DMemoryBIP(w,h,1,rawCopy.getDestination());
	}

	@Override public int[] getPixelInt(int i, int j, int[] iArray) {
		
		if(iArray == null)
			iArray = new int[getBandNumber()];
		
		for(int b = 0 ; b < getBandNumber() ; b++)
			iArray[b] = this.bands.getInt(getIndex(i,j,b));
		
		return iArray;
	}

	@Override public long[] getPixelLong(int i, int j, long[] lArray) {
		
		if(lArray == null)
			lArray = new long[getBandNumber()];
		
		for(int b = 0 ; b < getBandNumber() ; b++)
			lArray[b] = this.bands.getLong(getIndex(i,j,b));

		return lArray;
	}

	@Override public float[] getPixelFloat(int i, int j, float[] fArray) {
		
		if(fArray == null)
			fArray = new float[getBandNumber()];
		
		for(int b = 0 ; b < getBandNumber() ; b++)
			fArray[b] = this.bands.getFloat(getIndex(i,j,b));
		
		return fArray;
	}

	@Override public double[] getPixelDouble(int i, int j, double[] dArray) {
		
		if(dArray == null)
			dArray = new double[getBandNumber()];
		
		for(int b = 0 ; b < getBandNumber() ; b++)
			dArray[b] = this.bands.getDouble(getIndex(i,j,b));
		
		return dArray;
	}

	@Override public Object[] getPixelObject(int i, int j, Object[] oArray) {
		
		if(oArray == null)
			oArray = new Object[getBandNumber()];

		for(int b = 0 ; b < getBandNumber() ; b++)
			oArray[b] = this.bands.getObject(getIndex(i,j,b));
		
		return oArray;
	}

	@Override public Raster2D getPixels(int i, int j, int w, int h) {
		
		DataBufferRawCopy rawCopy = DataBufferFactory.rawCopy(this.bands,w * h * getBandNumber());
		
		for(int y = 0 ; y < h ; y++) {
			for(int x = 0 ; x < w ; x++) {
				for(int b = 0 ; b < getBandNumber() ; b++) {
					rawCopy.copy(getIndex(i+x, j+y, b), getIndex(w,getBandNumber(), x, y, b));
				}
			}
		}
		
		return new Raster2DMemoryBIP(w,h,getBandNumber(),rawCopy.getDestination());
	}
	
	@Override public void setSampleInt(int i, int j, int b, int s) {
		this.bands.setInt(getIndex(i,j,b),s);
	}

 	@Override public void setSampleLong(int i, int j, int b, long s) {
 		this.bands.setLong(getIndex(i,j,b),s);
	}

	@Override public void setSampleFloat(int i, int j, int b, float s) {
		this.bands.setFloat(getIndex(i,j,b),s);
	}

	@Override public void setSampleDouble(int i, int j, int b, double s) {
		this.bands.setDouble(getIndex(i,j,b),s);
	}

	@Override public void setSampleObject(int i, int j, int b, Object o) {
		this.bands.setObject(getIndex(i,j,b),o);
	}

	@Override public void setSamples(int i, int j, int b, Raster2D raster, int rasterBand) throws Exception {
		
		if(DataType.isIntType(getDataType())) {
			for(int y = 0 ; y < raster.getHeight() ; y++) {
				for(int x = 0 ; x < raster.getWidth() ; x++) {
					this.bands.setInt(getIndex(i+x,j+y,b),raster.getSampleInt(x, y, rasterBand));
				}
			}
		}
		
		else if(DataType.isLongType(getDataType())) {
			for(int y = 0 ; y < raster.getHeight() ; y++) {
				for(int x = 0 ; x < raster.getWidth() ; x++) {
					this.bands.setLong(getIndex(i+x,j+y,b),raster.getSampleLong(x, y, rasterBand));
				}
			}
		}
		
		else if(DataType.isFloatType(getDataType())) {
			for(int y = 0 ; y < raster.getHeight() ; y++) {
				for(int x = 0 ; x < raster.getWidth() ; x++) {
					this.bands.setFloat(getIndex(i+x,j+y,b),raster.getSampleFloat(x, y, rasterBand));
				}
			}
		}
		
		else if(DataType.isDoubleType(getDataType())) {
			for(int y = 0 ; y < raster.getHeight() ; y++) {
				for(int x = 0 ; x < raster.getWidth() ; x++) {
					this.bands.setDouble(getIndex(i+x,j+y,b),raster.getSampleDouble(x, y, rasterBand));
				}
			}
		}
		
		else if(DataType.isCustomType(getDataType())) {
			Object o = null;
			for(int y = 0 ; y < raster.getHeight() ; y++) {
				for(int x = 0 ; x < raster.getWidth() ; x++) {
					o = raster.getSampleObject(x, y, rasterBand, o);
					this.bands.setObject(getIndex(i+x,j+y,b),o);
				}
			}
		}
	}

	@Override public void setPixelInt(int i, int j, int[] iArray) throws Exception {
		for(int b = 0 ; b < getBandNumber() ; b++) {
			this.bands.setInt(getIndex(i,j,b),iArray[b]);
		}
	}

	@Override public void setPixelLong(int i, int j, long[] lArray) {
		for(int b = 0 ; b < getBandNumber() ; b++) {
			this.bands.setLong(getIndex(i,j,b),lArray[b]);
		}
	}

	@Override public void setPixelFloat(int i, int j, float[] fArray) {
		for(int b = 0 ; b < getBandNumber() ; b++) {
			this.bands.setFloat(getIndex(i,j,b),fArray[b]);
		}
	}

	@Override public void setPixelDouble(int i, int j, double[] dArray) {
		for(int b = 0 ; b < getBandNumber() ; b++) {
			this.bands.setDouble(getIndex(i,j,b),dArray[b]);
		}
	}

	@Override public void setPixelObject(int i, int j, Object[] oArray) {
		for(int b = 0 ; b < getBandNumber() ; b++) {
			this.bands.setObject(getIndex(i,j,b),oArray[b]);
		}
	}

	@Override public void setPixels(int i, int j, Raster2D raster) throws Exception {
		
		if(DataType.isIntType(getDataType())) {
			for(int y = 0 ; y < raster.getHeight() ; y++) {
				for(int x = 0 ; x < raster.getWidth() ; x++) {
					for(int b = 0 ; b < getBandNumber() ; b++) {
						this.bands.setInt(getIndex(i+x,j+y,b),raster.getSampleInt(x,y,b));
					}
				}
			}
		}
		
		else if(DataType.isLongType(getDataType())) {
			for(int y = 0 ; y < raster.getHeight() ; y++) {
				for(int x = 0 ; x < raster.getWidth() ; x++) {
					for(int b = 0 ; b < getBandNumber() ; b++) {
						this.bands.setLong(getIndex(i+x,j+y,b),raster.getSampleLong(x,y,b));
					}
				}
			}
		}
		
		else if(DataType.isFloatType(getDataType())) {
			for(int y = 0 ; y < raster.getHeight() ; y++) {
				for(int x = 0 ; x < raster.getWidth() ; x++) {
					for(int b = 0 ; b < getBandNumber() ; b++) {
						this.bands.setFloat(getIndex(i+x,j+y,b),raster.getSampleFloat(x, y, b));
					}
				}
			}
		}
		
		else if(DataType.isDoubleType(getDataType())) {
			for(int y = 0 ; y < raster.getHeight() ; y++) {
				for(int x = 0 ; x < raster.getWidth() ; x++) {
					for(int b = 0 ; b < getBandNumber() ; b++) {
						this.bands.setDouble(getIndex(i+x,j+y,b),raster.getSampleDouble(x, y, b));
					}
				}
			}
		}
		
		else if(DataType.isCustomType(getDataType())) {
			Object o = null;
			for(int y = 0 ; y < raster.getHeight() ; y++) {
				for(int x = 0 ; x < raster.getWidth() ; x++) {
					for(int b = 0 ; b < getBandNumber() ; b++) {
						o = raster.getSampleObject(x, y, b, o);
						this.bands.setObject(getIndex(i+x,j+y,b),o);
					}
				}
			}
		}
		
	}
	
	
	@Override public void close() {
		//nothing to do
	}

}
