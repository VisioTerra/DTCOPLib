package fr.visioterra.dtcop;

import java.io.File;

import com.bc.zarr.Compressor;
import com.bc.zarr.chunk.ChunkReaderWriter;

import fr.visioterra.lib.data.RasterNDNetCDF;
import fr.visioterra.lib.format.dtcop.zarr.DTCOPCompressor;
import fr.visioterra.lib.format.dtcop.zarr.ZarrWriter;
import fr.visioterra.lib.format.netcdf.NetCDFFile;

public class Transcode {

	//Transcode NetCDF to DTCOP/Zarr
	public static void function01() throws Exception {
		
		int len = 384;
		double maxError = 1.0;
		int threadNumber = 10;
		
		
		File inputFile = new File("D:/temp/ERA5/BIG/t2m/t2m_2021_adaptor.mars.internal-1700398056.3011448-21416-15-fba6f16c-0921-4a9a-8079-8078a81dc132.nc");
		String varId = "t2m";
		File outputFile = new File("D:/temp/ERA5/BIG/t2m/t2m_2021_DTCOP_maxError_1.0.zarr");
		
		
		ZarrWriter.debug = true;
		ZarrWriter zw = new ZarrWriter(outputFile.getAbsolutePath());
		
		NetCDFFile.debug = true;
		NetCDFFile ncfile = new NetCDFFile(inputFile);
		
		long start = System.currentTimeMillis();
		
		try(RasterNDNetCDF raster = ncfile.getRaster("time")) {
			zw.encodeVariable(raster.getVariable(), raster, null);
		}
		
		try(RasterNDNetCDF raster = ncfile.getRaster("latitude")) {
			zw.encodeVariable(raster.getVariable(), raster, null);
		}
		
		try(RasterNDNetCDF raster = ncfile.getRaster("longitude")) {
			zw.encodeVariable(raster.getVariable(), raster, null);
		}
		
//		try(RasterNDNetCDF raster = ncfile.getRaster(varId); RasterNDBufferedCache cache = new RasterNDBufferedCache(raster, new int[] {len,721,1440}, 2 * 1024L * 1024 * 1024)) {
//		try(RasterNDNetCDF raster = ncfile.getRaster(varId); RasterNDBufferedCache cache = new RasterNDBufferedCache(raster, new int[] {len,len,len}, 2 * 1024L * 1024 * 1024)) {
		try(RasterNDNetCDF raster = ncfile.getRaster(varId)) {
			
			int[] chunkSize = new int[] {len,len,len};

			Compressor compressor = new DTCOPCompressor(null, maxError / raster.getScaleFactor(), threadNumber);
			
//			ChunkReaderWriter.readVersion = 1;
			ChunkReaderWriter.writeVersion = 1;
//			FileSystemStore.readVersion = 1;
//			FileSystemStore.writeVersion = 1;
			
//			zw.encodeVariable(raster.getVariable(), cache, chunkSize, null, compressor);
			zw.encodeVariable(raster.getVariable(), raster, chunkSize, null, compressor);
			
//			System.out.println(Arrays.toString(zw.browse));
			System.out.println("nb call  = " + zw.browse[0] + " / total time = " + zw.browse[1] / 1000000);
			System.out.println("nb read  = " + zw.browse[2] + " / read time  = " + zw.browse[3] / 1000000);
			System.out.println("nb write = " + zw.browse[4] + " / write time = " + zw.browse[5] / 1000000);
			
//			filter.dumpDctStats();
		}

		long duration = System.currentTimeMillis() - start;
		
		System.out.println("Duration = " + duration + "ms");
		
	}
	
	
	
	
	
	public static void main(String[] args) throws Exception {
		function01();
	}
	
}
