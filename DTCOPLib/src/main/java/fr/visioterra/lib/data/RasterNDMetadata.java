package fr.visioterra.lib.data;


import java.util.List;

import ucar.nc2.Dimension;

public interface RasterNDMetadata {
	
//	public List<Variable> getVariables();

	List<Dimension> getDimensions();
	
	int[] getShape();
	
	int getLength(int dimension);
	
	int getLength(String dimension);
	
	int getDataType();
	
	public double getScaleFactor();
	
	public double getAddOffset();	
	
}
