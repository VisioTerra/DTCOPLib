package fr.visioterra.lib.format.dtcop.shard;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;

import fr.visioterra.lib.format.dtcop.chunk.Cell;
import fr.visioterra.lib.format.dtcop.chunk.Chunk;
import fr.visioterra.lib.format.dtcop.chunk.ChunkReader;
import fr.visioterra.lib.format.dtcop.huffman.Huffman;
import fr.visioterra.lib.io.bit.BitReader;
import fr.visioterra.lib.io.stream.StreamTools;

public class ShardReader {

	
	
	
	//3D - read methods
	private static Chunk readChunk(InputStream is, Huffman huffman) {
		return null;
	}
    
	public static Chunk read(InputStream is) throws Exception {
		
		byte[] magicNumber = new byte[5];
		is.read(magicNumber);
		
		//magic number + version
		System.out.println("magic       : " + new String(magicNumber));
		System.out.println("version     : " + is.read());
		
		//ID header + size
		System.out.println("ID header   : " + is.read());
		System.out.println("size        : " + StreamTools.readShort(is));
		
		//Shard shape & Chunk shape
		int len = is.read();

		int[] shape = new int[len];
		for(int i = 0 ; i < shape.length ; i++) {
			shape[i] = StreamTools.readShort(is);
		}
		System.out.println("Shape       : " + Arrays.toString(shape));
		
		int[] chunkShape = new int[len];
		for(int i = 0 ; i < chunkShape.length ; i++) {
			chunkShape[i] = StreamTools.readShort(is);
		}
		System.out.println("Chunk shape : " + Arrays.toString(chunkShape));
		
		//huffman table
		System.out.println("ID header   : " + is.read());
		int symbolCount = StreamTools.readInt(is);
		int size = StreamTools.readInt(is);		
		System.out.println("symbol count: " + symbolCount);
		System.out.println("size        : " + size);
		byte[] data = new byte[size];
		StreamTools.readFully(is,data,0,data.length);
		
		Huffman huffman = null;
		try(BitReader br = new BitReader(new ByteArrayInputStream(data))) {
			huffman = new Huffman().readTable(br, symbolCount, 16);
		}
//		huffman.print();
		
		
		//chunks table
		System.out.println("ID header   : " + is.read());
		int chunkNumber = StreamTools.readShort(is);
		int[] chunkSizeArray = new int[chunkNumber];
		for(int i = 0 ; i < chunkNumber ; i++) {
			chunkSizeArray[i] = StreamTools.readInt(is);
		}

//		ChunkQuant cq = new ChunkQuant(shape, quantPolynom);
		Cell[] order = Cell.order(chunkShape,true);
		
		
		ArrayList<byte[]> chunkDataList = new ArrayList<>();
		for(int i = 0 ; i < chunkNumber ; i++) {
			data = new byte[chunkSizeArray[i]];
			StreamTools.readFully(is, data, 0, data.length);
			chunkDataList.add(data);
			
			
			Chunk chunk = ChunkReader.getChunk(new ByteArrayInputStream(data),huffman,order);
			if(i == 0) {
//				chunk.print(true);
				return chunk;
			}
			
		}
		
		
		
		return null;
	}
	
	
	
	
	
}
