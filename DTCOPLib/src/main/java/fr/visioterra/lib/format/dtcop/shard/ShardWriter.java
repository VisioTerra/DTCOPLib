package fr.visioterra.lib.format.dtcop.shard;

import java.io.OutputStream;
import java.util.ArrayList;
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

	public static final byte[] magicNumber = new byte[] { 68, 84, 67, 79, 80}; 
	public static final byte idVersion  = 0x00;
	public static final byte idHeader   = 0x01;
	public static final byte idHuffTab  = 0x02;
	public static final byte idChunkTab = 0x03;
	
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
					idx++;
					
					//get chunk as FLOAT Array
					Chunk origin = shard.getChunk(idxChunk, DataType.FLOAT);
					
					//create a runnable and submit
					Runnable runnable = new Runnable() {
						@Override public void run() {
							ChunkWriter cw = new ChunkWriter(origin, qChunks, maxError, cells, histogram);
							synchronized(chunkWriterMap) {
								chunkWriterMap.put(index,cw);
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
	
	private static void writeHeader(OutputStream os, int[] shape, int[] chunkShape) throws Exception {
		
		int headerSize = 1 + 4 * shape.length;
		
		os.write(idHeader);
		StreamTools.writeShort(os, headerSize);
		
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
	}
	
	private static void writeHuffmanTable(OutputStream os, Huffman huffman) throws Exception {
		
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try(BitWriter bw = new BitWriter(baos,64)) {
			huffman.writeTable(bw,16);
		}
		byte[] huffmanTableData = baos.toByteArray();

		
		os.write(idHuffTab);
		StreamTools.writeInt(os, huffman.getSymbolCount());
		StreamTools.writeInt(os, huffmanTableData.length);
		os.write(huffmanTableData);
		
	}
	
	private static void writeChunks(OutputStream os, ArrayList<byte[]> chunkDataList) throws Exception {
		
		os.write(idChunkTab);
		
		StreamTools.writeShort(os,chunkDataList.size());
		for(byte[] data : chunkDataList) {
			StreamTools.writeInt(os,data.length);
		}
		
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

		
		ArrayList<byte[]> chunkDataList = new ArrayList<>();
		for(ChunkWriter cw : chunkWriterMap.values()) {
			byte[] data = cw.getBytes(huffman);
			chunkDataList.add(data);
		}
		
		writeMagic(os);
		writeHeader(os, shard.getShape(), shard.getChunkShape());
		writeHuffmanTable(os, huffman);
		writeChunks(os, chunkDataList);
		
	}
	
}
