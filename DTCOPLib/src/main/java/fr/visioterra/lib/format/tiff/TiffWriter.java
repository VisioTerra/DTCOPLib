package fr.visioterra.lib.format.tiff;

import java.awt.Point;
import java.awt.Rectangle;
import java.awt.Transparency;
import java.awt.color.ColorSpace;
import java.awt.image.ColorModel;
import java.awt.image.ComponentColorModel;
import java.awt.image.DataBuffer;
import java.awt.image.Raster;
import java.awt.image.RenderedImage;
import java.awt.image.SampleModel;
import java.awt.image.WritableRaster;
import java.io.OutputStream;
import java.util.Vector;

import javax.imageio.ImageTypeSpecifier;

import org.apache.xmlgraphics.image.codec.tiff.TIFFEncodeParam;
import org.apache.xmlgraphics.image.codec.tiff.TIFFImageEncoder;

import fr.visioterra.lib.image.dataBuffer.DataType;
import fr.visioterra.lib.image.raster2D.Raster2D;

public class TiffWriter {

	public static class RasterRenderedImage implements RenderedImage {
		
		private final Raster2D raster;
		private final ImageTypeSpecifier imageTypeSpecifier;
		
		public RasterRenderedImage(Raster2D raster) throws Exception {
			
			this.raster = raster;
			
			int bandNumber = raster.getBandNumber();
			int dataType   = raster.getDataType();
			int dataSize   = DataType.getSize(dataType);

			if (DataType.isIntType(dataType) == true && DataType.getSize(dataType) <= 16) {
				if(bandNumber == 1) {
					this.imageTypeSpecifier = ImageTypeSpecifier.createGrayscale(dataSize, getDataBufferType(dataType), DataType.isSignedType(dataType));
				} else if (bandNumber == 3){
					this.imageTypeSpecifier = ImageTypeSpecifier.createInterleaved(ColorSpace.getInstance(ColorSpace.CS_LINEAR_RGB), new int[] { 0, 1, 2 }, getDataBufferType(dataType), false, false);
				}
				else {
					throw new UnsupportedOperationException("Invalid band number for data type " + DataType.getName(dataType) + ".");
				}
			}
			
			else if (DataType.isRealType(dataType) == true && bandNumber == 1) {
				ComponentColorModel colorModel = new ComponentColorModel(ColorSpace.getInstance(ColorSpace.CS_GRAY),false,false, Transparency.OPAQUE, getDataBufferType(DataType.TYPE_FLOAT));
				this.imageTypeSpecifier = new ImageTypeSpecifier(colorModel,colorModel.createCompatibleSampleModel(raster.getWidth(), raster.getHeight()));
			}
			
			else {
				throw new UnsupportedOperationException("Invalid data type or band number.");
			}

		}

		@Override public int getWidth() {
			return this.raster.getWidth();
		}

		@Override public int getHeight() {
			return this.raster.getHeight();
		}

		@Override public int getMinX() {
			return 0;
		}

		@Override public int getMinY() {
			return 0;
		}

		@Override public int getNumXTiles() {
			return 1;
		}

		@Override public int getNumYTiles() {
			return 1;
		}

		@Override public int getTileWidth() {
			return this.raster.getWidth();
		}

		@Override public int getTileHeight() {
			return this.raster.getHeight();
		}
		
		@Override public ColorModel getColorModel() {
			return this.imageTypeSpecifier.getColorModel();
		}

		@Override public SampleModel getSampleModel() {
			return this.imageTypeSpecifier.getSampleModel().createCompatibleSampleModel(getWidth(),getHeight());
		}

		@Override public Raster getData(Rectangle rect) {
			
			if(rect.x % getTileWidth() == 0 && rect.y % getTileHeight() == 0 && rect.width == getTileWidth() && rect.height == getTileHeight()) {
				int tileX = rect.x / getTileWidth();
				int tileY = rect.y / getTileHeight();
				return getTile(tileX, tileY);
			}
			
			throw new UnsupportedOperationException("getData(origin=" + rect.x + "," + rect.y + " / dimensions=" + rect.width + "," + rect.height + ")");
		}
		
		@Override public Vector<RenderedImage> getSources() {
			throw new UnsupportedOperationException();
		}

		@Override public Object getProperty(String name) {
			throw new UnsupportedOperationException();
		}

		@Override public String[] getPropertyNames() {
			throw new UnsupportedOperationException();
		}

		@Override public int getMinTileX() {
			throw new UnsupportedOperationException();
		}

		@Override public int getMinTileY() {
			throw new UnsupportedOperationException();
		}

		@Override public int getTileGridXOffset() {
			throw new UnsupportedOperationException();
		}

		@Override public int getTileGridYOffset() {
			throw new UnsupportedOperationException();
		}

		@Override public Raster getTile(int tileX, int tileY) {
			
			try {
				
				int tileWidth = getTileWidth();
				int tileHeight = getTileHeight();
				SampleModel sm = imageTypeSpecifier.getSampleModel().createCompatibleSampleModel(tileWidth,tileHeight);
				
				int originX = 0;
				int originY = 0;
				WritableRaster outputTile = Raster.createWritableRaster(sm, new Point(originX,originY));
				
				if (DataType.isIntType(this.raster.getDataType())) {
					int[] array = new int[this.raster.getBandNumber()];
					for(int j = 0 ; j < this.raster.getHeight() ; j++) {
						for(int i = 0 ; i < this.raster.getWidth() ; i++) {
							int[] pix = this.raster.getPixelInt(i,j,array);
							outputTile.setPixel(originX + i, originY + j, pix);
						}
					}
				} else {
					float[] array = new float[this.raster.getBandNumber()];
					for(int j = 0 ; j < this.raster.getHeight() ; j++) {
						for(int i = 0 ; i < this.raster.getWidth() ; i++) {
							float[] pix = this.raster.getPixelFloat(i,j,array);
							outputTile.setPixel(originX + i, originY + j, pix);
						}
					}
				}

				return outputTile;
			} catch(Exception e) {
				e.printStackTrace();
				throw new RuntimeException(e);
			}

		}

		@Override public Raster getData() {
			throw new UnsupportedOperationException();
		}

		@Override public WritableRaster copyData(WritableRaster raster) {
			throw new UnsupportedOperationException();
		}
		
		public static int getDataBufferType(int dataType) throws IllegalArgumentException {

			switch (dataType) {
				case (DataType.TYPE_INT8):
				case (DataType.TYPE_UINT8):
					return DataBuffer.TYPE_BYTE;
				case (DataType.TYPE_INT16):
					return DataBuffer.TYPE_SHORT;
				case (DataType.TYPE_UINT16):
					return DataBuffer.TYPE_USHORT;
				case (DataType.TYPE_INT32):
					return DataBuffer.TYPE_INT;
				case (DataType.TYPE_FLOAT):
					return DataBuffer.TYPE_FLOAT;
				case (DataType.TYPE_DOUBLE):
					return DataBuffer.TYPE_DOUBLE;
				default:
					throw new IllegalArgumentException("Unsupported data type");
			}
		}
		
	}

	
	
	/**
	 * description: Extracts the GeoKeys of the GeoTIFF. The Following Tags will
	 * be extracted(http://www.remotesensing.org/geotiff/spec/geotiffhome.html):
	 * <ul>
	 * <li>ModelPixelScaleTag = 33550 (SoftDesk)
	 * <li>ModelTiepointTag = 33922 (Intergraph)
	 * </ul>
	 * implementation status: working
	 */

	public void encode(Raster2D raster, OutputStream os) throws Exception {
		
		if(raster == null) {
			throw new NullPointerException("raster == null");
		}
		
		TIFFEncodeParam tep = new TIFFEncodeParam();
		tep.setWriteTiled(true);
		
		TIFFImageEncoder encoder = new TIFFImageEncoder(os, tep);
		encoder.encode(new RasterRenderedImage(raster));
		os.flush();
	}
	
}
