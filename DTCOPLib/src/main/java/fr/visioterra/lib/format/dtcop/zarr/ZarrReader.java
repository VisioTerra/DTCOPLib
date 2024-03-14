package fr.visioterra.lib.format.dtcop.zarr;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.bc.zarr.ZarrArray;
import com.bc.zarr.ZarrGroup;
import com.bc.zarr.storage.Store;

import fr.visioterra.lib.data.ChunkedRasterNDDTCOP;
import fr.visioterra.lib.data.ChunkedRasterNDZarr;

public class ZarrReader {
	
	private final ZarrGroup zarrGroup;
	

	public ZarrReader(File rootDir) throws IOException {
		this.zarrGroup = ZarrGroup.open(rootDir.getAbsolutePath());
	}

	public ZarrReader(Store store) throws IOException {
		this.zarrGroup = ZarrGroup.open(store);
	}
	
	public Collection<String> getVariables() throws IOException {
		return new ArrayList<String>(this.zarrGroup.getArrayKeys());
		
	}
	
	public int[] getShape(String varName) throws IOException {
		ZarrArray array = zarrGroup.openArray(varName);
		return array.getShape();
	}
	
	public ChunkedRasterNDZarr getRaster(String varName) throws IOException {
		return getRaster(varName, null);
	}
	
	public ChunkedRasterNDZarr getRaster(String varName, ZarrFilter filter) throws IOException {
		return new ChunkedRasterNDZarr(this.zarrGroup, varName, filter);
	}
	
	public ChunkedRasterNDDTCOP getRaster(String varName, ZarrFilter filter, int[] forceChunkShape) throws IOException {
		return new ChunkedRasterNDDTCOP(this.zarrGroup, varName, filter, forceChunkShape);
	}
	
	public static List<String> getArrayDimensions(ZarrArray zarrArray) throws IOException {
		String attributeName = "_ARRAY_DIMENSIONS";
		
		Object object = zarrArray.getAttributes().get(attributeName);

		List<String> dimensions = new ArrayList<>();
		
		if (object instanceof Collection<?>) {
			Collection<?> collection = (Collection<?>) object;
			
			for (Object item : collection) {
				if (item instanceof String) {
					dimensions.add((String) item);
				}
				else {
					throw new IllegalArgumentException("");
				}
			}
		}
		else {
			throw new IllegalArgumentException();
		}
		
		return dimensions;
	}

	public boolean hasGeoDimensions(String varName) throws IOException {
		
		ZarrArray array = zarrGroup.openArray(varName);
		
		if (array == null) throw new IllegalArgumentException("Invalid var name \"" + varName + "\"");
		
		List<String> dimensions = getArrayDimensions(array);
		
		boolean hasLongitude = false;
		boolean hasLatitude  = false;
		
		for (String dimension : dimensions) {
			switch (dimension.toLowerCase()) {
				case "lon":
				case "long":
				case "longitude": hasLongitude = true; break;
				case "lat":
				case "latitude": hasLatitude = true; break;
			}
		}
		
		return hasLongitude && hasLatitude;
	}
	
}
