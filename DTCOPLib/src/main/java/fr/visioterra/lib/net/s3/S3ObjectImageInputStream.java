package fr.visioterra.lib.net.s3;

import java.io.IOException;

import javax.imageio.stream.ImageInputStreamImpl;

import fr.visioterra.lib.io.stream.AtomicSeekableInputStream;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

public class S3ObjectImageInputStream extends ImageInputStreamImpl implements AtomicSeekableInputStream {

	public static boolean debug = false;
	private final Object lock = new Object();
	private final S3Client client;
	private final String bucketName;
	private final String key;
	
	public S3ObjectImageInputStream(S3Client client, String bucketName, String key) {
		this.client = client;
		this.bucketName = bucketName;
		this.key = key;
	}
	
	@Override public int read() throws IOException {
		byte[] array = new byte[1];
		int ret = read(array,0,1);
		if(ret <= 0) {
			return ret;
		}
		else {
			return array[0];
		}
	}

	@Override public int read(byte[] b, int off, int len) throws IOException {
		
		synchronized (this.lock) {
			
			long pos = this.getStreamPosition();
			String range = "bytes=" + pos + "-" + (pos+len);
			
			GetObjectRequest getObjectRequest = GetObjectRequest.builder().bucket(bucketName).key(key).range(range).build();
			
			try(ResponseInputStream<GetObjectResponse> ris = this.client.getObject(getObjectRequest)) {
				
//				long start = System.currentTimeMillis();
				long start = System.nanoTime();
				int total = 0;
				int size = ris.read(b,off,len);
				while(size >= 0 && len > 0) {
					off = off + size;
					len = len - size;
					total += size;
					size = ris.read(b,off,len);
				}
				
				this.seek(pos + total);
				
				if(debug) {
//					long duration = System.currentTimeMillis() - start;
					long duration = System.nanoTime() - start;
					if(duration == 0) {
						duration = 1;
					}
//					double rate = (total * 1000.0) / (duration * 1024 * 1024);
					long rate = (total * 1_000_000_000L) / (duration * 1024 * 1024);
					System.out.println(this.getClass().getSimpleName() + ".read(byte[] b, int off, int len) : Download range " + range + " - " + total + " bytes in " + duration + " ms - " + rate + " MB/s");
				}
				
				return total;
			}
		}
		
	}
	
	@Override public byte[] read(long pos, byte[] b, int off, int len) throws IOException {
		
		String range = "bytes=" + pos + "-" + (pos+len);

		GetObjectRequest getObjectRequest = GetObjectRequest.builder().bucket(bucketName).key(key).range(range).build();

		try(ResponseInputStream<GetObjectResponse> ris = this.client.getObject(getObjectRequest)) {

//			long start = System.currentTimeMillis();
			long start = System.nanoTime();
			int total = 0;
			int size = ris.read(b,off,len);
			while(size >= 0 && len > 0) {
				off = off + size;
				len = len - size;
				total += size;
				size = ris.read(b,off,len);
			}

			this.seek(pos + total);

			if(debug) {
//				long duration = System.currentTimeMillis() - start;
				long duration = System.nanoTime() - start;
				if(duration == 0) {
					duration = 1;
				}
//				double rate = (total * 1000.0) / (duration * 1024 * 1024);
				long rate = (total * 1_000_000_000L) / (duration * 1024 * 1024);
				System.out.println(this.getClass().getSimpleName() + ".read(long pos, byte[] b, int off, int len) : Download range " + range + " - " + total + " bytes in " + duration + " ms - " + rate + " MB/s");
			}

			return b;
		}
	}
	
	@Override public void close() throws IOException {
		super.close();
		this.client.close();
	}
	
	
	
	/*
	
	
	
		private static class MyCacheImageInputStream extends ImageInputStreamImpl { // implements AtomicSeekableInputStream {

		private final Object lock = new Object();
		private final S3Client client;
		private final String bucketName;
		private final String key;
		private final long length;
		
//		private static final int cacheMaxSize = 32 * 1024;
		
		private long cacheOriginPos = -1;
		private final byte[] cache = new byte[32 * 1024]; //cacheMaxSize];
		private int cacheSize = -1;
		private int cachePos = -1;
		
		private int readS3(long pos, byte[] b, int off, int len) throws IOException {
			
			String range = "bytes=" + pos + "-" + (pos+len);

			GetObjectRequest getObjectRequest = GetObjectRequest.builder().bucket(bucketName).key(this.key).range(range).build();

			try(ResponseInputStream<GetObjectResponse> ris = this.client.getObject(getObjectRequest)) {

				long start = System.currentTimeMillis();
				int total = 0;
				int size = ris.read(b,off,len);
				while(size >= 0 && len > 0) {
					off = off + size;
					len = len - size;
					total += size;
					size = ris.read(b,off,len);
				}

				if(debug) {
					long duration = System.currentTimeMillis() - start;
					if(duration == 0) {
						duration = 1;
					}
					double rate = (total * 1000.0) / (duration * 1024 * 1024);
					System.out.println("\t" + this.getClass().getSimpleName() + ".readS3(long pos, byte[] b, int off, int len) : Download range " + range + " - " + total + " bytes in " + duration + " ms - " + rate + " MB/s");
				}

				return total;
			}
		}

		private void clearCache() {
			this.cacheOriginPos = -1;
			this.cacheSize = -1;
			this.cachePos = -1;
		}
		
		private void loadCache(long pos) throws IOException {
			int len = this.cache.length;
			if(0 < this.length && this.length < (pos + len)) {
				len = (int)(this.length - pos);
			}
			this.cacheOriginPos = pos;
			this.cacheSize = readS3(pos, this.cache, 0, len);
			this.cachePos = 0;
		}
		
		private boolean isCacheLoaded() {
			return this.cacheSize > -1;
		}
		
		public MyCacheImageInputStream(S3Client client, String bucketName, String key) {
			this(client,bucketName,key,-1L);
		}
		
		public MyCacheImageInputStream(S3Client client, String bucketName, String key, long length) {
			this.client = client;
			this.bucketName = bucketName;
			this.key = key;
			this.length = length;
		}
		
		@Override public int read() throws IOException {
			byte[] array = new byte[1];
			int ret = read(array,0,1);
			if(ret <= 0) {
				return ret;
			}
			else {
				return array[0];
			}
		}

		@Override public int read(byte[] b, int off, int len) throws IOException {
			
//			if(debug) {
//				System.out.println(this.getClass().getSimpleName() + ".read(byte[] b, int off, int len) : b.length=" + b.length + ", off=" + off + ",len=" + len);
//			}
			
			//use cache ?
			if(len < this.cache.length) {
				synchronized (this.lock) {
					
					int remaining = this.cacheSize - this.cachePos;
					long pos = this.getStreamPosition();
					
					if(isCacheLoaded() == false || len > remaining) {
						loadCache(pos);
					}

					System.arraycopy(this.cache, this.cachePos, b, off, len);
					this.seek(pos + len);
					return len;
				}
			}
			else {
				synchronized (this.lock) {
					clearCache();
					long pos = this.getStreamPosition();
					int read = readS3(pos, b, off, len);
					this.seek(pos + read);
					return read;
				}
			}
			
		}
		
		public byte[] read(long pos, byte[] b, int off, int len) throws IOException {
			
			if(debug) {
				System.out.println(this.getClass().getSimpleName() + ".read(long pos, byte[] b, int off, int len) : pos=" + pos + ", b.length=" + b.length + ", off=" + off + ",len=" + len);
			}
			
			readS3(pos, b, off, len);
			return b;
		}
		
	    @Override public long length() {
	        return this.length;
	    }
	    
		@Override public void seek(long pos) throws IOException {
			
//			if(debug) {
//				System.out.println(this.getClass().getSimpleName() + ".seek(long pos) : pos=" + pos);
//			}
			
			super.seek(pos);
			
			if(isCacheLoaded()) {
				if(this.cacheOriginPos < pos && pos < this.cacheOriginPos + this.cacheSize) {
					this.cachePos = (int)(pos - this.cacheOriginPos);
				}
				else {
					clearCache();
				}
			}
			
		}
		
		@Override public void close() throws IOException {
			super.close();
			this.client.close();
		}

	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	*/
	
}
