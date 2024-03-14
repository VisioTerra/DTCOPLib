package fr.visioterra.lib.format.dtcop.zarr;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONObject;

import com.bc.zarr.ArrayParams;
import com.bc.zarr.Compressor;
import com.bc.zarr.CompressorFactory;
import com.bc.zarr.DataType;
import com.bc.zarr.ZarrArray;
import com.bc.zarr.ZarrGroup;

import fr.visioterra.lib.data.RasterND;
import ucar.ma2.Array;
import ucar.nc2.Attribute;
import ucar.nc2.AttributeContainer;
import ucar.nc2.Dimension;
import ucar.nc2.Variable;

public class ZarrWriter {

	public static boolean debug = false;
	private final String path;
	private final ZarrGroup root;
	
	public long[] browse = new long[6]; //{ nb call, total time, nb read, read time, nb write, write time } 
	
	
	private void createZattrs(Variable variable) throws Exception {
		createZattrs(variable.getDimensions(), variable.attributes(), variable.getFullName());
	}
	
	private void createZattrs(List<Dimension> dimensions, AttributeContainer attributes, String fullName) throws Exception {
//		if(!METADATA) return;
		
//		String fullName = variable.getFullName();
		JSONArray array_dimensions = new JSONArray();
		for(Dimension dim : dimensions) {
			array_dimensions.put(dim.getName());
		}
		JSONObject zattrs = new JSONObject().put("_ARRAY_DIMENSIONS", array_dimensions);
		
		
		for(Attribute att : attributes) {
			zattrs.put(att.getName(), att.getValue(0));
		}
		
		File file = new File(this.path + File.separator + fullName + File.separator + ".zattrs");
		
		if(file.getParentFile().exists() == false) {
			file.getParentFile().mkdirs();
		}
		
		try(PrintStream ps = new PrintStream(file, "UTF-8")) {
//			ps.append(varName+":"+filter.getClass().getSimpleName());
			ps.println(zattrs.toString());
		}
	}
	
	private void browse(RasterND raster, ZarrArray zArray, int[] chunkSize, ZarrFilter filter) throws Exception {
		int len      = raster.getShape().length;
		int[] origin = new int[len];
		int[] shape  = new int[len];
		browse(raster, zArray, chunkSize, 0, origin, shape, filter);
	}
	
	private void browse(RasterND raster, ZarrArray zArray, int[] chunkShape, int dim, int[] origin, int[] shape, ZarrFilter filter) throws Exception {
		
		long start = System.nanoTime();
		
		//increment nb of call
		this.browse[0]++;
		
		int[] rasterShape = raster.getShape();

		if(chunkShape == null) {
			chunkShape = rasterShape;
		}
		
		int maxChunkIndex = rasterShape[dim] / chunkShape[dim];
		
		if(rasterShape[dim] % chunkShape[dim] != 0) {
			maxChunkIndex++;
		}
		
		this.browse[1] += (System.nanoTime() - start);
		
//		if(debug) {
//			StringBuilder sb = new StringBuilder("rasterShape = [");
//			boolean coma = false;
//			for(int i = 0 ; i < rasterShape.length ; i++) {
//				if(coma) {
//					sb.append(",");
//				}
//				sb.append(rasterShape[i]);
//				coma = true;
//			}
//			
//			sb.append("] / chunkShape = [");
//			
//			coma = false;
//			for(int i = 0 ; i < chunkShape.length ; i++) {
//				if(coma) {
//					sb.append(",");
//				}
//				sb.append(chunkShape[i]);
//				coma = true;
//			}
//			sb.append("] / maxChunkIndex = ");
//			sb.append(maxChunkIndex);
//			
//			System.out.println(sb.toString());
//		}
		
		
		
		for(int chunkIndex = 0 ; chunkIndex < maxChunkIndex ; chunkIndex++) {
			
			start = System.nanoTime();

			origin[dim] = chunkIndex * chunkShape[dim];
			shape[dim]  = Math.min(chunkShape[dim] , rasterShape[dim] - origin[dim]);
			
			
			StringBuilder sb = null;
			
			if(debug){
				sb = new StringBuilder();
				
				for(int i = 0 ; i < dim ; i++) {
					sb.append("    ");
				}
				
				sb.append("dim idx = " + dim + " / " + chunkShape.length
						+ "  -  chunk idx = " + chunkIndex + " / " + maxChunkIndex
						+ "  -  position = " + origin[dim] + " + " + shape[dim] + " / " + rasterShape[dim]);
			}
			
			this.browse[1] += (System.nanoTime() - start);
			
			if(dim == chunkShape.length - 1) {
				
				start = System.nanoTime();
				
				this.browse[2]++;
				long readStart = System.nanoTime();
				Array array = raster.getArray(origin, shape);
				this.browse[3] += (System.nanoTime() - readStart);
				
				
//				if(debug){
//
//					sb.append(" - chunk shape = [");
//					
//					boolean coma = false;
//					for(int i = 0 ; i < shape.length ; i++) {
//						if(coma) { sb.append(","); }
//						sb.append(shape[i]);
//						coma = true;
//					}
//					sb.append("]");
//					
//				}
				
				this.browse[4]++;
				long writeStart = System.nanoTime();				
				if(filter == null) {
					zArray.write(array, shape, origin);	
				}
				else {
//					if (debug) sb.append(" - filter = ").append(filter.getClass().getSimpleName());
					Array tmp = filter.encode(array);
					zArray.write(tmp, shape, origin);
				}
				this.browse[5] += (System.nanoTime() - writeStart);
				
//				if (debug) System.out.println(sb.toString());
				
				this.browse[1] += (System.nanoTime() - start);
			}
			else {

				if (debug) System.out.println(sb.toString());
				browse(raster, zArray, chunkShape, dim+1, origin, shape, filter);
			}
			
		}
		
	}
	
	public ZarrWriter(String path) throws IOException {
		this.path = path;
		this.root = ZarrGroup.create(path);
	}
	
	public void encodeVariable(Variable variable, RasterND raster, int[] chunkSize) throws Exception {
		Compressor compressor = CompressorFactory.create("zlib", "level", 9);
//		Compressor compressor = CompressorFactory.nullCompressor;
		encodeVariable(variable, raster, chunkSize, null, compressor);
	}

	public void encodeVariable(Variable variable, RasterND raster, int[] chunkSize, ZarrFilter filter, Compressor compressor) throws Exception {
		
		int[] fullShape = raster.getShape();
		
		ArrayParams params = new ArrayParams();
		params.shape(fullShape);
		params.compressor(compressor);
		params.dataType(getDataType(raster.getDataType()));
//		params.fillValue(fillValue)
		
		if(chunkSize == null) {
			params.chunked(false);
		}
		else {
			params.chunked(true);
			params.chunks(chunkSize);			
		}
		
		ZarrArray zArray = this.root.createArray(variable.getFullName(), params);
//		zArray.writeAttributes(attributes);
		createZattrs(variable);
		
//		if(chunkSize != null) {
			browse(raster, zArray, chunkSize, filter);
//		}
//		else {
//			Array array = raster.getArray(origin, shape);
//			zArray.write(array, shape, origin); //origin,  shape);
//		}
		
		
		
		
//		int[] fullShape = variable.getDimensions().stream().mapToInt(d -> d.getLength()).toArray();
//		ArrayParams arrayParams = new ArrayParams().shape(fullShape).compressor(compressor).dataType(DataType.i2).fillValue(getFillValue(variable));
//		
//		int[] chunks = getChunks(variable);
//		if(chunks == null) { // auto chunks
//			arrayParams.chunked(true);
//		}
//		else { // user-defined chunks
//			arrayParams.chunks(chunks);
//		}
//		
//		ZarrArray zarrArray = root.createArray(variable.getFullName(), arrayParams);
//		createZattrs(variable);
//		
//		if(fullShape.length >= 3 && fullShape[0] >= 2000) { // ZarrCompression for large 3D variables
//			int iterations = 10; // number of "cuts" in the 3D variable
//			Iterator<int[][]> it = ZarrCDF.iteratorFor3DCuts(fullShape[2], fullShape[1], fullShape[0], iterations);
//			while(it.hasNext()) {
//				int[][] cut = it.next();
//				//System.out.println(cut[0][0] + " " + (cut[1][0]+cut[0][0]));
//				writeInZarr(zarrArray, variable, cut[1], cut[0]);
//			}
//		}
//		else { // ZarrCompression for 1D, 2D, small 3D and small 4D variables
//			int[] origin = IntStream.range(0, fullShape.length).map(i -> 0).toArray();
//			writeInZarr(zarrArray, variable, fullShape, origin);
//		}

		
		
		
	}
	
	public void encodeVariable(List<Dimension> dimensions, AttributeContainer attributes, String fullName, RasterND raster, int[] chunkSize, ZarrFilter filter, Compressor compressor) throws Exception {
		
		int[] fullShape = raster.getShape();
		
		ArrayParams params = new ArrayParams();
		params.shape(fullShape);
		params.compressor(compressor);
		params.dataType(getDataType(raster.getDataType()));
//		params.fillValue(fillValue)
		
		if(chunkSize == null) {
			params.chunked(false);
		}
		else {
			params.chunked(true);
			params.chunks(chunkSize);			
		}
		
		ZarrArray zArray = this.root.createArray(fullName, params);
//		zArray.writeAttributes(attributes);
		createZattrs(dimensions,attributes,fullName);
		
//		if(chunkSize != null) {
			browse(raster, zArray, chunkSize, filter);
//		}
//		else {
//			Array array = raster.getArray(origin, shape);
//			zArray.write(array, shape, origin); //origin,  shape);
//		}
		
		
		
		
//		int[] fullShape = variable.getDimensions().stream().mapToInt(d -> d.getLength()).toArray();
//		ArrayParams arrayParams = new ArrayParams().shape(fullShape).compressor(compressor).dataType(DataType.i2).fillValue(getFillValue(variable));
//		
//		int[] chunks = getChunks(variable);
//		if(chunks == null) { // auto chunks
//			arrayParams.chunked(true);
//		}
//		else { // user-defined chunks
//			arrayParams.chunks(chunks);
//		}
//		
//		ZarrArray zarrArray = root.createArray(variable.getFullName(), arrayParams);
//		createZattrs(variable);
//		
//		if(fullShape.length >= 3 && fullShape[0] >= 2000) { // ZarrCompression for large 3D variables
//			int iterations = 10; // number of "cuts" in the 3D variable
//			Iterator<int[][]> it = ZarrCDF.iteratorFor3DCuts(fullShape[2], fullShape[1], fullShape[0], iterations);
//			while(it.hasNext()) {
//				int[][] cut = it.next();
//				//System.out.println(cut[0][0] + " " + (cut[1][0]+cut[0][0]));
//				writeInZarr(zarrArray, variable, cut[1], cut[0]);
//			}
//		}
//		else { // ZarrCompression for 1D, 2D, small 3D and small 4D variables
//			int[] origin = IntStream.range(0, fullShape.length).map(i -> 0).toArray();
//			writeInZarr(zarrArray, variable, fullShape, origin);
//		}

		
		
		
	}
	
	
	
	public static com.bc.zarr.DataType getDataType(int dataType) throws IllegalArgumentException {

		switch(dataType) {
			
			case(fr.visioterra.lib.image.dataBuffer.DataType.TYPE_UINT8): {
				return DataType.u1;
			}
			
			case(fr.visioterra.lib.image.dataBuffer.DataType.TYPE_INT8): {
				return DataType.i1;
			}
			
			case(fr.visioterra.lib.image.dataBuffer.DataType.TYPE_UINT16): {
				return DataType.u2;
			}
			
			case(fr.visioterra.lib.image.dataBuffer.DataType.TYPE_INT16): {
				return DataType.i2;
			}
			
			case(fr.visioterra.lib.image.dataBuffer.DataType.TYPE_INT32): {
				return DataType.i4;
			}
			
			case(fr.visioterra.lib.image.dataBuffer.DataType.TYPE_INT64): {
				return DataType.i8;
			}
			
			case(fr.visioterra.lib.image.dataBuffer.DataType.TYPE_FLOAT): {
				return DataType.f4;
			}
			
			case(fr.visioterra.lib.image.dataBuffer.DataType.TYPE_DOUBLE): {
				return DataType.f8;
			}
			
			default: {
				throw new IllegalArgumentException("Unsupported dataType : " + fr.visioterra.lib.image.dataBuffer.DataType.getName(dataType) + " (" + dataType + ")");
			}
			
		}
		
	}
	
	
}
