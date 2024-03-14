package fr.visioterra.lib.format.dtcop.shard;

import java.io.File;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.HashMap;

import fr.visioterra.lib.format.dtcop.chunk.Cell;
import fr.visioterra.lib.format.dtcop.chunk.Chunk;
import fr.visioterra.lib.format.dtcop.chunk.ChunkWriter;
import fr.visioterra.lib.format.dtcop.huffman.Huffman;
import fr.visioterra.lib.format.dtcop.quantization.QuantChunk;
import fr.visioterra.lib.io.bit.BitReader;
import fr.visioterra.lib.io.stream.ByteArrayInputStream;
import fr.visioterra.lib.io.stream.StreamTools;
import ucar.ma2.DataType;



public class ShardReader implements AutoCloseable {
	
	private static class BlockEntry {

		private final long offset;
		private final long size;
		
		public BlockEntry(long offset, long size) {
			this.offset = offset;
			this.size = size;
		}
		
		public long getOffset() {
			return this.offset;
		}

		public long getSize() {
			return this.size;
		}
		
		@Override public String toString() {
			return "[offset=" + offset + " size=" + size + "]";
		}
		
	}
	
	
	public static boolean debug = false;
	
	private final Object lock = new Object();
	private final RandomAccessFile raf;
	private final HashMap<Byte,BlockEntry> blockEntryMap = new HashMap<>();
	private final DataType dataType;
	private final int[] shape;
	private final int[] chunkShape;
	private final int[] numChunk;
	private final Cell[] zigzagOrder;
	private final Huffman huffman;
	private final int[] chunksSize;
	private final long[] chunksOffset;
	private final long chunksStartPosition;
	
	
	
 	private static void readMagic(InputStream is) throws Exception {
		byte[] head = new byte[5];
		is.read(head);
		
		for(int i = 0 ; i < head.length ; i++) {
			if(head[i] != ShardWriter.magicNumber[i]) {
				throw new IllegalArgumentException("Invalid magic number");
			}
		}
		
		int version = is.read();
		
		if(version != ShardWriter.idVersion) {
			throw new IllegalArgumentException("Invalid version (" + version + " != " + ShardWriter.idVersion);
		}
		
	}
	
	private static int readHeaderTag(InputStream is, int idBlock) throws Exception {
		
		int id = is.read();
		
		if(id != idBlock) {
			throw new IllegalArgumentException("Invalid version (" + id + " != " + idBlock);
		}
		
		return StreamTools.readInt(is); 
	}
	
	private static Huffman readHuffmanTable(InputStream is) throws Exception {
		
		if(debug) {
			System.out.println("readHuffmanTable");
		}
		
		int blockSize = readHeaderTag(is, ShardWriter.idBlockHuffmanTable);
		
		int symbolLen = is.read();
		int symbolCount = StreamTools.readInt(is);
		
		if(debug) {
			System.out.println("Block size = " + blockSize + " / symbolLen = " + symbolLen + " / symbolCount = " + symbolCount);
		}
		
		Huffman huffman = new Huffman();
		try(BitReader br = new BitReader(is)) {
			huffman.readTable(br, symbolCount, symbolLen);
		}
		
//		int huffmanTableSize = blockSize - 1 - 4;
//		byte[] huffmanTableData = new byte[huffmanTableSize];
//		
//		StreamTools.readFully(is, huffmanTableData, 0, huffmanTableData.length);
//		
//		Huffman huffman = new Huffman();
//		try(BitReader br = new BitReader(new ByteArrayInputStream(huffmanTableData))) {
//			huffman.readTable(br, symbolCount, symbolLen);
//		}
		
		if(debug) {
			huffman.print();
		}
		
		return huffman;
	}
	
	private static int[] readChunksTable(InputStream is) throws Exception {
		
		if(debug) {
			System.out.println("readChunksTable");
		}
		
		int blockSize = readHeaderTag(is, ShardWriter.idBlockChunksTable);
		
		int chunksTableLen = StreamTools.readShort(is);
		
		if(blockSize != (2 + 4 * chunksTableLen)) {
			throw new IllegalArgumentException("Invalid chunks table len");
		}
		
		int[] chunksTableSize = new int[chunksTableLen];
		for(int i = 0 ; i < chunksTableSize.length ; i++) {
			chunksTableSize[i] = StreamTools.readInt(is);
			
			if(debug) {
				System.out.println("Chunk size = " + chunksTableSize[i] + " / chunksTableLen = " + chunksTableLen);
			}
			
		}
		return chunksTableSize;
		
	}
	
	private ByteArrayInputStream readBlock(RandomAccessFile raf, byte blockId) throws Exception {
		BlockEntry be = this.blockEntryMap.get(blockId);
		raf.seek(be.getOffset());
		byte[] array = new byte[(int)be.getSize()];
		raf.readFully(array);
		return new ByteArrayInputStream(array);
	}
	
	private ByteArrayInputStream readBlock(RandomAccessFile raf, BlockEntry be) throws Exception {
		byte[] array = new byte[(int)be.getSize()];
		synchronized (this.lock) {
			raf.seek(be.getOffset());
			raf.readFully(array);
		}
		return new ByteArrayInputStream(array);
	}
	
	public ShardReader(File file, DataType dataType) throws Exception {

		this.raf = new RandomAccessFile(file,"r");
		this.dataType = dataType;

		byte[] bytes = new byte[6];
		this.raf.readFully(bytes);
		readMagic(new ByteArrayInputStream(bytes));
		
		//read blocks id + size
		for(int i = 0 ; i < 4 ; i++) {
			long blockPos = this.raf.getFilePointer();
			byte blockId  = this.raf.readByte();
			int blockSize = this.raf.readInt() + 4 + 1;
			
			if(debug) {
				System.out.println("Block id = " + String.format("%02x:",blockId) + " / pos = " + blockPos + " / size = " + blockSize);
			}
			
			this.blockEntryMap.put(blockId, new BlockEntry(blockPos,blockSize));
			this.raf.seek(blockPos + blockSize);
		}
		
		//header
		try(InputStream is = readBlock(this.raf, ShardWriter.idBlockHeader)){
			
			int blockSize = readHeaderTag(is, ShardWriter.idBlockHeader);
			
			int dim = is.read();
			
			if(dim < 0 || 9 < dim) {
				throw new IllegalArgumentException("Invalid dimension number");
			}
			
			if(blockSize != (1 + dim * 2 + dim * 2)) {
				throw new IllegalArgumentException("Invalid header size");
			}
			
			this.shape = new int[dim];
			for(int i = 0 ; i < this.shape.length ; i++) {
				this.shape[i] = StreamTools.readShort(is);
			}
			
			this.chunkShape = new int[dim];
			for(int i = 0 ; i < this.chunkShape.length ; i++) {
				this.chunkShape[i] = StreamTools.readShort(is);
			}
			
			this.numChunk = new int[this.chunkShape.length];
			for(int i = 0 ; i < this.chunkShape.length ; i++) {
				int size = this.shape[i];
				int csize = this.chunkShape[i];
				this.numChunk[i] = size  % csize  == 0 ? size  / csize  : size  / csize  + 1;
			}
			
			this.zigzagOrder = Cell.order(this.chunkShape,true);
		}
		
		//Huffman table
		try(InputStream is = readBlock(this.raf, ShardWriter.idBlockHuffmanTable)) {
			this.huffman = readHuffmanTable(is);
		}
		
		//Chunks table
		{
			BlockEntry be = this.blockEntryMap.get(ShardWriter.idBlockChunksTable);
			try(InputStream is = readBlock(this.raf, be)) {
				this.chunksSize = readChunksTable(is);
			}
		}
		
		//Chunks data
		{
			BlockEntry be = this.blockEntryMap.get(ShardWriter.idBlockChunks);
			try(InputStream is = readBlock(this.raf, be)) {
				
				this.chunksStartPosition = be.getOffset() + 4 + 1;
				this.chunksOffset = new long[chunksSize.length];				
				
				long offset = this.chunksStartPosition;
				for(int i = 0 ; i < this.chunksSize.length ; i++) {
					this.chunksOffset[i] = offset;
					offset += this.chunksSize[i];
				}
				
			}
		}
		
			
			//test
//			int idx = 0; //this.chunksSize.length - 1;
//			byte[] tmp = new byte[this.chunksSize[idx]];
//			raf.seek(this.chunksOffset[idx]);
//			raf.readFully(tmp,0,tmp.length);
//			Chunk chunk = ChunkReader.getChunk(new ByteArrayInputStream(tmp), huffman, this.zigzagOrder);
//			chunk.print(false);
			

		
		
		
		
		
		
		
		
		


		
		
		/*
		this.dataType = dataType;
		
		try(BufferedInputStream bis = new BufferedInputStream(new FileInputStream(file), 16*1024)) {
			readMagic(bis);	
			
			//header
			{
				int blockSize = readHeaderTag(bis, ShardWriter.idBlockHeader);
				
				int dim = bis.read();
				
				if(dim < 0 || 9 < dim) {
					throw new IllegalArgumentException("Invalid dimension number");
				}
				
				if(blockSize != (1 + dim * 2 + dim * 2)) {
					throw new IllegalArgumentException("Invalid header size");
				}
				
				this.shape = new int[dim];
				for(int i = 0 ; i < this.shape.length ; i++) {
					this.shape[i] = StreamTools.readShort(bis);
				}
				
				this.chunkShape = new int[dim];
				for(int i = 0 ; i < this.chunkShape.length ; i++) {
					this.chunkShape[i] = StreamTools.readShort(bis);
				}
				
				this.numChunk = new int[this.chunkShape.length];
				for(int i = 0 ; i < this.chunkShape.length ; i++) {
					int size = this.shape[i];
					int csize = this.chunkShape[i];
					this.numChunk[i] = size  % csize  == 0 ? size  / csize  : size  / csize  + 1;
				}
				
			}
			
			//Huffman table
			this.huffman = readHuffmanTable(bis);
			
			readChunksTable(bis);
			
		}
		*/
		
	}
	
	public DataType getDataType() {
		return this.dataType;
	}
	
	public int[] getShape() {
		return this.shape;
	}
	
	public int[] getChunkShape() {
		return this.chunkShape;
	}
	
	public int[] getNumChunk() {
		return this.numChunk;
	}
	
	public int getNumChunk(int dim) {
		return this.numChunk[dim];
	}
	
	public Chunk getChunk(int[] chunkIdx) throws Exception {
		
//		int cidx = 0;
//		for(int i = 0 ; i < this.chunkShape.length ; i++) {
//			cidx = cidx * this.chunkShape[i] + chunkIdx[i];
//		}

		int cidx = 0;
		for(int i = 0 ; i < this.numChunk.length ; i++) {
			cidx = cidx * this.numChunk[i] + chunkIdx[i];
		}
		
//		System.out.println("getChunk(" + Arrays.toString(chunkIdx) + ") => " + cidx + " / " + this.chunksOffset.length);
		
		
		BlockEntry be = new BlockEntry(this.chunksOffset[cidx],this.chunksSize[cidx]);
		
		try(InputStream is = readBlock(this.raf, be)) {
			
			//TODO : check EOF

			//read chunk shape
			int[] shape = new int[is.read()];
			int size = 1;
			for(int d = 0 ; d < shape.length ; d++) {
				shape[d] = is.read();
				size = size * shape[d];
			}

			//read quantization polynom
			float[] quantPolynom = new float[is.read()];
			for(int d = 0 ; d < quantPolynom.length ; d++) {
				quantPolynom[d] = Float.intBitsToFloat(StreamTools.readInt(is));
			}

			//create output array for coefficients in zig zag order
			int[] zz = new int[size];

			//read DC coefficient
			zz[0] = StreamTools.readInt(is);

			QuantChunk cq = new QuantChunk(shape, quantPolynom);

			try(BitReader br = new BitReader(is)) {

				//Start at idx = 1 because position 0 is already initialized with DC coef
				int idx = 1; 
				while(idx < size - 1) {

					zz[idx] = huffman.readSymbol(br);

					if(zz[idx] == ChunkWriter.rleCode) {
						int s = (short)br.readBits(16);
						for(int i = 0 ; i < s ; i++) {
							zz[idx] = 0;
							idx++;
						}
					}
					else {
						idx++;
					}

				}
			}

			Chunk chunk = new Chunk(DataType.FLOAT, shape, zz, this.zigzagOrder);
			chunk.scale(cq,true,false);
			chunk.idct();
			return chunk;
			
			
//			int idx = 0; //this.chunksSize.length - 1;
//			byte[] tmp = new byte[this.chunksSize[idx]];
//			raf.seek(this.chunksOffset[idx]);
//			raf.readFully(tmp,0,tmp.length);
//			Chunk chunk = ChunkReader.getChunk(new ByteArrayInputStream(tmp), huffman, this.zigzagOrder);
//			chunk.print(false);
		}
	}
	
	@Override public void close() throws Exception {
		this.raf.close();
	}
	
}
