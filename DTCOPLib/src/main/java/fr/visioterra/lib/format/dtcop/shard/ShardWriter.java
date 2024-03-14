package fr.visioterra.lib.format.dtcop.shard;

import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import fr.visioterra.lib.format.dtcop.chunk.Cell;
import fr.visioterra.lib.format.dtcop.chunk.Chunk;
import fr.visioterra.lib.format.dtcop.chunk.ChunkWriter;
import fr.visioterra.lib.format.dtcop.huffman.Histogram;
import fr.visioterra.lib.format.dtcop.huffman.Huffman;
import fr.visioterra.lib.format.dtcop.quantization.QuantChunk;
import fr.visioterra.lib.io.bit.BitWriter;
import fr.visioterra.lib.io.stream.ByteArrayOutputStream;
import fr.visioterra.lib.io.stream.StreamTools;
import ucar.ma2.DataType;

public class ShardWriter {

	public static boolean debug = false;
	public static final byte[] magicNumber        = new byte[] { 68, 84, 67, 79, 80}; 
	public static final byte idVersion            = 0x00;
	public static final byte idBlockHeader        = 0x21;
	public static final byte idBlockHuffmanTable  = 0x22;
	public static final byte idBlockChunksTable   = 0x24;
	public static final byte idBlockChunks        = 0x28;
	public static final int huffmanTableSymbolLen = 16;
	
	
	
	private static Huffman computeHuffmanTable(Shard shard, List<QuantChunk> qChunks, double maxError, int threadNumber, TreeMap<Integer,ChunkWriter> chunkWriterMap) throws Exception {
		
		//16bits tuned
		Histogram histogram = new Histogram(256*256,Short.MAX_VALUE);
		
		//zigzag order as Cell[] 
		Cell[] cells = Cell.order(shard.getChunkShape(),true);
		
		//Thread pool executor
		ThreadPoolExecutor tpe = new ThreadPoolExecutor(threadNumber, threadNumber, 1, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());

		//loop on chunks
		int[] numChunk = shard.getNumChunk();
		int[] idxChunk = new int[] {0,0,0};
		int idx = 0;
		
		final Object lock = new Object();
		
		//loop on k dim
		for(int k = 0 ; k < numChunk[0] ; k++) {
			idxChunk[0] = k;
			
			//loop on j dim
			for(int j = 0 ; j < numChunk[1] ; j++) {
				idxChunk[1] = j;
				
				//loop on i dim
				for(int i = 0 ; i < numChunk[2] ; i++) {
					idxChunk[2] = i;

					final int index = idx;
					
					//TODO : remove debug
//					System.out.println(Arrays.toString(idxChunk) + " => " + idx);
					
					idx++;
					
					//get chunk as FLOAT Array
					Chunk origin = shard.getChunk(idxChunk, DataType.FLOAT);
					
					//create a runnable and submit
					Runnable runnable = new Runnable() {
						@Override public void run() {
							synchronized(lock) {
								try {
									chunkWriterMap.put(index,new ChunkWriter(origin, qChunks, maxError, cells, histogram));
								} catch(Exception e) {
									System.err.println(e.getMessage());
								}
							}
						}
					};
					tpe.submit(runnable);
					
				}
			}
		}

		//wait completion
		tpe.shutdown();
		while (tpe.awaitTermination(50, TimeUnit.MILLISECONDS) == false) {}
		
		//create Huffman table
		return histogram.getHuffman();
	}
	
	private static void writeMagic(OutputStream os) throws Exception {
		os.write(magicNumber);
		os.write(idVersion);
	}
	
	private static void writeHeaderTag(OutputStream os, byte idBlock, int blockSize) throws Exception {
		os.write(idBlock);
		StreamTools.writeInt(os,blockSize);
	}
	
	private static void writeHeader(OutputStream os, int[] shape, int[] chunkShape) throws Exception {
		
		if(debug) {
			System.out.println("writeHeader");
		}
		
		int dim = shape.length;
		
		int blockSize = 1 + 2 * dim + 2 *dim;
		writeHeaderTag(os, idBlockHeader, blockSize);
		
		//1 - write array dimension number
		os.write((byte)shape.length);

		//6 (2*DIM - 3D=2x3) - write array shape
		for(int d : shape) {
			StreamTools.writeShort(os, d);
		}

		//6 (2*DIM - 3D=2x3) - write chunk shape
		for(int d : chunkShape) {
			StreamTools.writeShort(os, d);
		}
		
		if(debug) {
			System.out.println("Shape = " + Arrays.toString(shape) + " / ChunkShape = " + Arrays.toString(chunkShape));
		}
		
	}
	
	private static void writeHuffmanTable(OutputStream os, Huffman huffman, int symbolLen) throws Exception {
		
		if(debug) {
			System.out.println("writeHuffmanTable");
		}
		
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try(BitWriter bw = new BitWriter(baos,64)) {
			huffman.writeTable(bw,16);
		}
		byte[] huffmanTableData = baos.toByteArray();

		int blockSize = 1 + 4 + huffmanTableData.length;
		writeHeaderTag(os, idBlockHuffmanTable,  blockSize);
		
		os.write(symbolLen);
		StreamTools.writeInt(os, huffman.getSymbolCount());
		os.write(huffmanTableData);
		
		if(debug) {
			System.out.println("Block size = " + blockSize);
		}
	}
	
	private static void writeChunksTable(OutputStream os, ArrayList<byte[]> chunkDataList) throws Exception {
		
		if(debug) {
			System.out.println("writeChunksTable / count = " + chunkDataList.size());
		}
		
		int blockSize = 2 + 4 * chunkDataList.size();
		writeHeaderTag(os, idBlockChunksTable, blockSize);
		
		StreamTools.writeShort(os,chunkDataList.size());
		for(byte[] data : chunkDataList) {
			StreamTools.writeInt(os,data.length);
			if(debug) {
				System.out.println("Chunk size = " + data.length);
			}
		}
		
	}
	
	private static void writeChunks(OutputStream os, ArrayList<byte[]> chunkDataList) throws Exception {
		
		int blockSize = 0;
		for(byte[] data : chunkDataList) {
			blockSize += data.length;
		}
		
		writeHeaderTag(os, idBlockChunks, blockSize);
		
		for(byte[] data : chunkDataList) {
			os.write(data);
		}
		
	}
	
	public static void write(OutputStream os, Shard shard, List<QuantChunk> qChunks, double maxError, int threadNumber) throws Exception {
		
		if(shard.getShape().length != 3) {
			throw new UnsupportedOperationException();
		}
		
		//chunks as int[]
		TreeMap<Integer,ChunkWriter> chunkWriterMap = new TreeMap<>();
		
		//create Huffman table
		Huffman huffman = computeHuffmanTable(shard, qChunks, maxError, threadNumber, chunkWriterMap);

		int count = 1;
		int[] numChunk = shard.getNumChunk();
		for(int i = 0 ; i < numChunk.length ; i++) {
			count = count * numChunk[i];
		}
		
		if(count != chunkWriterMap.size()) {
			throw new Exception("Invalid processed chunk number (expected=" + count + " / written=" + chunkWriterMap.size() + ")");
		}

		if(debug) {
			System.out.println("ShardWriter.write(...) : " + chunkWriterMap.size() + " chunks");
		}
		
		ArrayList<byte[]> chunkDataList = new ArrayList<>();
		for(ChunkWriter cw : chunkWriterMap.values()) {
			byte[] data = cw.getBytes(huffman);
			chunkDataList.add(data);
		}
		
		writeMagic(os);
		writeHeader(os, shard.getShape(), shard.getChunkShape());
		writeHuffmanTable(os, huffman, huffmanTableSymbolLen);
		writeChunksTable(os, chunkDataList);
		writeChunks(os, chunkDataList);
		
	}
	
}
