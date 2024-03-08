package fr.visioterra.lib.format.netcdf;

import java.io.File;
import java.util.Collection;
import java.util.LinkedHashMap;

import fr.visioterra.lib.data.RasterNDNetCDF;
import ucar.nc2.Attribute;
import ucar.nc2.NetcdfFile;
import ucar.nc2.NetcdfFiles;
import ucar.nc2.Variable;

public class NetCDFFile {
	
	public static boolean debug = false;
	private final File inputFile;
	private final LinkedHashMap<String,Variable> variableMap;
	
	
	public NetCDFFile(File inputFile) throws Exception {
		
		this.inputFile = inputFile;
		
		if(false == NetcdfFiles.canOpen(inputFile.getAbsolutePath())) {
			throw new Exception("Cannot open file " + inputFile.getAbsolutePath() + " (Unrecognize as NetCDF file)");
		}
		
		try(NetcdfFile ncfile = NetcdfFiles.open(inputFile.getAbsolutePath())) {
		
			this.variableMap = new LinkedHashMap<>();
			
			for(Variable v : ncfile.getVariables()) {
				
				this.variableMap.put( v.getFullName(),v);
				
				if(debug) {
					System.out.println(v.getNameAndDimensions() + " - " + v.getDescription() + " - " + v.getSize() + " - " + v.getDataType() + " - " + v.getShapeAsSection());
					for(Attribute attr : v.attributes()) {
						System.out.println("\t" + attr.toString());
						System.out.println("\t" + attr.getName() + " - " + attr.getValues() + " - " + attr.getDataType() + " - " + attr.getEnumType());
					}
				}
				
			}
			
		}
		
	}
	
	public boolean containsVariable(String varName) {
		return this.variableMap.containsKey(varName);
	}
	
	public Collection<Variable> getVariables() {
//		return ncfile.getVariables();
		return this.variableMap.values();
	}
	
	public int[] getShape(String varName) {
		
		Variable v = this.variableMap.get(varName);
		
		if(v == null) {
			throw new IllegalArgumentException("Invalid variable name (" + varName + ")");
		}
		
		return v.getShape();
	}
	
	public RasterNDNetCDF getRaster(String id) throws Exception {
		return new RasterNDNetCDF(this.inputFile, id);
	}
	
	public RasterNDNetCDF getRaster(String id, int overrideDataType) throws Exception {
		return new RasterNDNetCDF(this.inputFile, id, overrideDataType);
	}
	
}
