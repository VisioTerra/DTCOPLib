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
		double maxError = 2.0;
		int threadNumber = 8;
		
		
//		File inputFile = new File("D:/temp/ERA5/BIG/t2m/t2m_2022_adaptor.mars.internal-1700398997.4559674-5320-14-a2ea3ea8-cd11-497e-8563-0043d837dd51.nc");
//		File inputFile = new File("D:/temp/ERA5/BIG/t2m/t2m_2021_adaptor.mars.internal-1700398056.3011448-21416-15-fba6f16c-0921-4a9a-8079-8078a81dc132.nc");
//		File inputFile = new File("D:/temp/ERA5/BIG/t2m/t2m_2020_adaptor.mars.internal-1700448475.829057-16746-10-ae99e3b4-c0d0-4e2a-9d7a-c99d5a1f1419.nc");
//		File inputFile = new File("E:/ERA5/t2m/t2m_2019_adaptor.mars.internal-1700479595.0714147-22340-3-d3ef7af8-77ee-4493-aed5-07f5b19865f7.nc");
//		File inputFile = new File("E:/ERA5/t2m/t2m_2018_adaptor.mars.internal-1700439035.6144273-5287-2-3b7e177d-8471-46da-8e7f-30e0341c0d16.nc");
//		File inputFile = new File("E:/ERA5/t2m/t2m_2017_adaptor.mars.internal-1700431416.3697078-6035-9-44f508ae-d76f-4510-82a9-1fc36edfaf71.nc");
		File inputFile = new File("E:/ERA5/t2m/t2m_2016_adaptor.mars.internal-1700421956.2104888-32762-5-47eccabe-75b8-427f-8ec6-9571422ac36c.nc");
//		File inputFile = new File("E:/ERA5/t2m/");
		
		
		String varId = "t2m";

//		File outputFile = new File("D:/temp/ERA5/BIG/t2m/t2m_2022_DTCOP_maxError_2.0.zarr");
//		File outputFile = new File("D:/temp/ERA5/BIG/t2m/t2m_2021_DTCOP_maxError_2.0.zarr");
//		File outputFile = new File("D:/temp/ERA5/BIG/t2m/t2m_2020_DTCOP_maxError_2.0.zarr");
//		File outputFile = new File("D:/temp/ERA5/BIG/t2m/t2m_2019_DTCOP_maxError_2.0.zarr");
//		File outputFile = new File("D:/temp/ERA5/BIG/t2m/t2m_2018_DTCOP_maxError_2.0.zarr");
//		File outputFile = new File("D:/temp/ERA5/BIG/t2m/t2m_2017_DTCOP_maxError_2.0.zarr");
		File outputFile = new File("D:/temp/ERA5/BIG/t2m/t2m_2016_DTCOP_maxError_2.0.zarr__");
		
		
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
