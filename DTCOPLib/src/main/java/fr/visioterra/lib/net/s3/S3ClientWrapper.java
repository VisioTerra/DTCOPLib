package fr.visioterra.lib.net.s3;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import fr.visioterra.lib.tools.TaskMonitor;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.profiles.ProfileFile;
import software.amazon.awssdk.profiles.ProfileFile.Type;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Uri;
import software.amazon.awssdk.services.s3.model.Bucket;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

public class S3ClientWrapper {

	public static boolean debug = false;
	
	private static final int progressFactor = 1024; //factor applied on size to avoid the integer overflow on TaskMonitor.setProgress()
	private static final int buffSize = 64 * 1024;
	private final S3ClientBuilder clientBuilder;
	private String bucketName;
	
	/**
	 * @param profileFile File containing the following information: (Profile ID, access key, secret key)
	 *  
	 * [default]
	 * aws_access_key_id=xxxXXxXxxXxxxxXXxxXx
	 * aws_secret_access_key=XxxxXXXxxXXxxXXxxXXxxxXXXxxxXxxxxxXxxxxX
	 * 
	 */
	public S3ClientWrapper(File profileFile, URI endPoint) {
		this(profileFile,endPoint,null);
	}
	
	public S3ClientWrapper(File profileFile, URI endPoint, String bucketName) {
		
		ProfileFile profile = ProfileFile.builder()
				.type(Type.CONFIGURATION)
				.content(profileFile.toPath())
				.build();
		
		ClientOverrideConfiguration overrideConfiguration = ClientOverrideConfiguration.builder()
				.defaultProfileFile(profile)
				.defaultProfileName("default")
				.apiCallTimeout(Duration.ofSeconds(5))
				.build();
		
		this.clientBuilder = S3Client.builder();
		this.clientBuilder.overrideConfiguration(overrideConfiguration);
		this.clientBuilder.region(Region.EU_CENTRAL_1);
		this.clientBuilder.endpointOverride(endPoint);
		
		this.bucketName = bucketName;
	}
	
	public S3Client getClient() {
		return clientBuilder.build();
	}
	
	public List<Bucket> listBuckets() {
		return getClient().listBuckets().buckets();
	}
	
	public String getBucket() {
		return this.bucketName;
	}

	public void setBucket(String bucketName) {
		this.bucketName = bucketName;
	}
	
	public ListObjectsV2Response list(String prefix) throws SdkException {
		
		try(S3Client client = getClient()) {
			ListObjectsV2Request listObjectsV2Request = ListObjectsV2Request.builder().bucket(bucketName).prefix(prefix).build();
			return client.listObjectsV2(listObjectsV2Request);
		}
		
	}
	
	//key example : "Sentinel-1/SAR/IW_GRDH_1S/2023/11/05/S1A_IW_GRDH_1SDV_20231105T172353_20231105T172418_051085_062905_EF81.SAFE/manifest.safe"
	public S3ObjectInputStream getInputStream(String key) throws SdkException {
		S3ObjectInputStream.debug = debug;
		return new S3ObjectInputStream(getClient(), bucketName, key);
	}

	//URI example : new URI("s3://eodata/Sentinel-1/SAR/IW_GRDH_1S/2023/11/05/S1A_IW_GRDH_1SDV_20231105T172353_20231105T172418_051085_062905_EF81.SAFE/manifest.safe")
	public S3ObjectInputStream getInputStream(URI uri) {
		S3Client client = getClient();
		S3Uri s3Uri = client.utilities().parseUri(uri);
		String bucket = s3Uri.bucket().get().toUpperCase();
		String key = s3Uri.key().get();
		S3ObjectInputStream.debug = debug;
		return new S3ObjectInputStream(client,bucket,key);
	}
	
	//experimental
	public S3ObjectImageInputStream getImageInputStream(String key) {
		S3ObjectImageInputStream.debug = debug;
		return new S3ObjectImageInputStream(getClient(),this.bucketName, key);
	}
	
	//experimental
	public S3ObjectImageInputStream getImageInputStream(URI uri) {
		S3Client client = getClient();
		S3Uri s3Uri = client.utilities().parseUri(uri);
		String bucket = s3Uri.bucket().get().toUpperCase();
		String key = s3Uri.key().get();
		S3ObjectImageInputStream.debug = debug;
		return new S3ObjectImageInputStream(client,bucket, key);
	}
	
	
	public void download(String prefix, File outputDir, TaskMonitor tm) throws SdkException, Exception {
		
		try(S3Client client = getClient()) {
			
			//list objects
			ListObjectsV2Request listObjectsV2Request = ListObjectsV2Request.builder().bucket(bucketName).prefix(prefix).build();
			ListObjectsV2Response listObjectsV2 = client.listObjectsV2(listObjectsV2Request);

			//check truncated
			if(listObjectsV2.isTruncated()) {
				throw new Exception("ListObjectsV2Response truncated, use pagnitation instead");
			}
			
			//compute total size
			long size = 0;
			for(S3Object obj : listObjectsV2.contents()) {
				if(obj.size() != null) {
					size += obj.size().longValue();
				}
			}
			
			//TaskMonitor
			if(tm != null) {
				tm.initProgress(0,(int)(size / progressFactor));
			}
			
			//download
			long length = 0;
			for(S3Object s3obj : listObjectsV2.contents()) {

				//s3obj.key();					//Key
				//s3obj.lastModified();			//LastModified
				//s3obj.eTag();					//ETag
				//s3obj.size();					//Size
				//s3obj.storageClassAsString();	//StorageClass

				String key = s3obj.key();
				String suffix = s3obj.key().substring(prefix.length());
//				long size = s3obj.size();

				File outputFile = new File(outputDir.getAbsolutePath() + File.separator + suffix);
				
				if(outputFile.getParentFile().exists() == false) {
					outputFile.getParentFile().mkdirs();
				}

				long start = System.currentTimeMillis();
				
				GetObjectRequest getObjectRequest = GetObjectRequest.builder().bucket(bucketName).key(key).build();
//				GetObjectRequest getObjectRequest = GetObjectRequest.builder().bucket("EODATA").key(key).range("bytes=0-67108864").build();

				try(ResponseInputStream<GetObjectResponse> ris = client.getObject(getObjectRequest); FileOutputStream fos = new FileOutputStream(outputFile)) {
					
					byte[] buff = new byte[buffSize];
					int len = ris.read(buff);
					while(len >= 0) {
						length += len;
						if(tm != null) {
							tm.setProgress((int)(length / progressFactor));
						}
						fos.write(buff, 0, len);
						len = ris.read(buff);
					}
				}
				
				if(debug) {
					long duration = System.currentTimeMillis() - start;
					double rate = (outputFile.length() * 1000) / (duration * 1024 * 1024);
					System.out.println("... " + outputFile.length() + " bytes in " + duration + " ms - " + rate + " MB/s to " + outputFile.getAbsolutePath());
				}

			}
			
		}
		
		
	}
	
	public void downloadAsZip(String prefix, File zipFile, TaskMonitor tm) throws SdkException, Exception {
		downloadAsZip(prefix, zipFile, tm, 5);
	}
	
	public void downloadAsZip(String prefix, File zipFile, TaskMonitor tm, int compressionLevel) throws SdkException, Exception {
		
		if (prefix.endsWith("/")) {
			throw new IllegalArgumentException("Prefix should not end with a '/'");
		}
		
		int factor = 1024;
		
		try(S3Client client = getClient()) {
			
			List<S3Object> filteredContents;
			
			{
				//list objects
				ListObjectsV2Request listObjectsV2Request = ListObjectsV2Request.builder().bucket(bucketName).prefix(prefix).build();
//				ListObjectsV2Request listObjectsV2Request = ListObjectsV2Request.builder().bucket(prefix).build();
				ListObjectsV2Response listObjectsV2 = client.listObjectsV2(listObjectsV2Request);
				
				//check truncated
				if(listObjectsV2.isTruncated()) {
					throw new Exception("ListObjectsV2Response truncated, use pagnitation instead");
				}
				
				// filter key
				filteredContents = listObjectsV2.contents().stream().filter(obj -> obj.key().startsWith(prefix + "/")).collect(Collectors.toList());

				if (listObjectsV2.contents().size() > 0) {
					if (filteredContents.size() == 0) {
						throw new IllegalArgumentException("Prefix is not a node.");
					}
				}
				else {
					throw new IllegalArgumentException("No entry found.");
				}
			}
			

			// create parentDir
			File parentDir = zipFile.getParentFile();
			if (parentDir.exists() == false) {
				parentDir.mkdirs();
			}
			
			File tmpFile = new File(zipFile + ".tmp");
			
			//compute total size
			long total = 0;
			for(S3Object obj : filteredContents) {
				if(obj.size() != null) {
					total += obj.size().longValue();
				}
			}
			
			//TaskMonitor
			if(tm != null) {
				tm.initProgress(0,(int)(total / factor));
			}
			
			//download
			long length = 0;
			
			try (ZipOutputStream zos = new ZipOutputStream(new BufferedOutputStream(new FileOutputStream(tmpFile),buffSize))) {
				zos.setLevel(compressionLevel);

				for(S3Object s3obj : filteredContents) {
					String key = s3obj.key();
					String suffix = s3obj.key().substring(prefix.lastIndexOf('/')+1);
					long size = s3obj.size();

					long start = System.currentTimeMillis();

					GetObjectRequest getObjectRequest = GetObjectRequest.builder().bucket(bucketName).key(key).build();

					ZipEntry entry = new ZipEntry(suffix);
					entry.setSize(size);
					zos.putNextEntry(entry);
					
					try(ResponseInputStream<GetObjectResponse> ris = client.getObject(getObjectRequest)) {

						byte[] buff = new byte[buffSize];
						int len = ris.read(buff);
						while(len >= 0) {
							length += len;
							if(tm != null) {
								tm.setProgress((int)(length / factor));
							}
							zos.write(buff, 0, len);
							len = ris.read(buff);
						}
					}
					
					zos.closeEntry();
					
					if(debug) {
//						entry.set
						long duration = System.currentTimeMillis() - start;
						double rate = (size * 1000) / (duration * 1024 * 1024);
						System.out.println("Download " + suffix + " : " + size + " bytes in " + duration + " ms - " + rate + " MB/s");
					}

				}

			}
			
			tmpFile.renameTo(zipFile);
		}
		
	}
	
	
}
