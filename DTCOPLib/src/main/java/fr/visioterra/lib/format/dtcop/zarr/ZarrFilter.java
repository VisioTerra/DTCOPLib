package fr.visioterra.lib.format.dtcop.zarr;

import ucar.ma2.Array;

public interface ZarrFilter {
	
	String getName();
	
	Array encode(Array array);
	
	Array decode(Array array);
	
}
