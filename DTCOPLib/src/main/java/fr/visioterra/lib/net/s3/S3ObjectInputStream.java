package fr.visioterra.lib.net.s3;

import java.io.IOException;
import java.io.InputStream;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

public class S3ObjectInputStream extends InputStream {
	
	public static boolean debug = false;
	private final S3Client client;
	private final ResponseInputStream<GetObjectResponse> ris;
	
	
	public S3ObjectInputStream(S3Client client, String bucket, String key) throws SdkException {
		GetObjectRequest gor = GetObjectRequest.builder().bucket(bucket).key(key).build();
		this.client = client;
		this.ris = client.getObject(gor);
		
		if(debug) {
			String acceptRanges = this.ris.response().acceptRanges();
			System.out.println(getClass().getSimpleName() + " - accept ranges: " + acceptRanges);
		}
		
	}
	
	public Long contentLength() {
		return this.ris.response().contentLength();
	}
	
	public int read() throws IOException {
		return this.ris.read();
	}

	public int read(byte b[]) throws IOException {
		return this.ris.read(b);
	}

	public int read(byte b[], int off, int len) throws IOException {
		return this.ris.read(b, off, len);
	}

	public long skip(long n) throws IOException {
		return this.ris.skip(n);
	}

	public int available() throws IOException {
		return this.ris.available();
	}

	public synchronized void mark(int readlimit) {
		this.ris.mark(readlimit);
	}

	public synchronized void reset() throws IOException {
		this.ris.reset();
	}

	public boolean markSupported() {
		return this.ris.markSupported();
	}

	public void close() throws IOException {
		this.ris.close();
		this.client.close();
	}
	
}