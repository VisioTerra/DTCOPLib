package com.bc.zarr.storage;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.bc.zarr.ZarrConstants;
import com.bc.zarr.ZarrUtils;

import fr.visioterra.lib.net.s3.S3ClientWrapper;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

public class S3Store implements Store {
	
	static class S3Path {

		private final String storeKey;
		private final List<String> parts;
		
//		private static List<String> split(String key) {
//			if (key.isEmpty()) {
//				return Collections.emptyList();
//			}
//			return Arrays.asList(key.split("/"));
//		}

		S3Path(List<String> storeKeyParts) {
			parts = storeKeyParts.stream()
					.filter(part -> part != null && !part.trim().isEmpty())
					.map(String::trim)
					.collect(Collectors.toList());
			this.storeKey = String.join("/", parts);
		}

		S3Path(String storeKey) {
			this.storeKey = storeKey.trim().isEmpty() ? "" : ZarrUtils.normalizeStoragePath(storeKey);
			
			if(this.storeKey.isEmpty()) {
				this.parts = Collections.emptyList();
			}
			else {
				this.parts = Arrays.asList(this.storeKey.split("/"));
			}
//			parts = split(this.storeKey);
		}

		boolean endsWith(S3Path suffix) {
			int thisSize = size();
			int suffixSize = suffix.size();
			if(suffixSize > thisSize) {
				return false;
			}
			return suffix.getParts().equals(parts.subList(thisSize - suffixSize, thisSize));
		}

		public List<String> getParts() {
			return parts;
		}

		public int size() {
			return parts.size();
		}

		public S3Path resolve(String name) {
			if (name.trim().isEmpty()) {
				return this;
			}
			return new S3Path(storeKey + "/" + ZarrUtils.normalizeStoragePath(name));
		}

		@Override public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			S3Path s3Path = (S3Path) o;
			return Objects.equals(storeKey, s3Path.storeKey);
		}

		@Override public int hashCode() {
			return Objects.hash(storeKey);
		}

		@Override public String toString() {
			return storeKey;
		}
		
	}
	
	
//  InputStream getInputStream(String key) throws IOException;
//  OutputStream getOutputStream(String key) throws IOException;
//  void delete(String key) throws IOException;
//  TreeSet<String> getArrayKeys() throws IOException;
//  TreeSet<String> getGroupKeys() throws IOException;
//  TreeSet<String> getKeysEndingWith(String suffix) throws IOException;
//  Stream<String> getRelativeLeafKeys(String key) throws IOException;
	
	public static boolean debug = false;
	private final S3ClientWrapper cdse;
	private final String prefix;
	
	private final S3Path keyPrefix;
//	public static int writeBufferSize = 8 * 1024;
//  private final Path internalRoot;
    
    
	
    public S3Store(S3ClientWrapper cdse, String keyPrefix) {
    	this.cdse = cdse;
    	this.keyPrefix = new S3Path(keyPrefix);
    	this.prefix = keyPrefix;
    }
    

    private TreeSet<String> getParentsOf(String suffix) throws IOException {
    	
    	if(debug) {
    		System.out.println(getClass().getSimpleName() + ".getParentsOf(" + suffix + ")");
    	}
    	
    	
    	//      return getKeysEndingWith(suffix).stream()
    	//              .map(s -> internalRoot.relativize(internalRoot.resolve(s).getParent()).toString())
    	//              .collect(Collectors.toCollection(TreeSet::new));

    	return getKeysEndingWith(suffix).stream()
    			.map(S3Path::new)
    			.map(S3Path::getParts)
    			.map(parts -> new S3Path(parts.subList(0, parts.size() - 1)))
    			.map(S3Path::toString)
    			.collect(Collectors.toCollection(TreeSet::new));

    }

    private Stream<S3Path> getObjects2(String prefix) {
    	
    	try(S3Client client = this.cdse.getClient()) {
    		
    		ListObjectsV2Request request = ListObjectsV2Request.builder().bucket(this.cdse.getBucket()).prefix(prefix).build();
    		
    		if(debug) {
    			System.out.println(getClass().getSimpleName() + ".getObjects(" + prefix + ")");
//    			client.listObjectsV2Paginator(request).stream().forEach(a -> {System.out.println("\t" + a);});
    			
    			
    			ListObjectsV2Iterable paginator = client.listObjectsV2Paginator(request);
    			for (S3Object s3obj : paginator.contents()) {
    				System.out.println(s3obj.key() + " / " + s3obj.lastModified() + " / " + s3obj.size());
    			}	
    			
    		}
    		
    		return client.listObjectsV2Paginator(request).stream()
    				.flatMap(response -> response.contents().stream())
        			.map(S3Object::key)
        			.map(S3Path::new);
    	}
    	
    }
    
    private Stream<S3Path> getObjectList(String prefix) {
    	
	   	try(S3Client client = this.cdse.getClient()) {
	
			LinkedList<S3Path> list = new LinkedList<>();
			ListObjectsV2Request request = ListObjectsV2Request.builder().bucket(this.cdse.getBucket()).prefix(prefix).build();
//			ListObjectsV2Iterable paginator = client.listObjectsV2Paginator(request);

    		if(debug) {
    			System.out.println(getClass().getSimpleName() + ".getObjectList(" + prefix + ")");
    		}
			
			for (S3Object s3obj : client.listObjectsV2Paginator(request).contents()) {
				
				if(debug) {
					System.out.println(s3obj.key() + " / " + s3obj.lastModified() + " / " + s3obj.size());
				}
				
				list.add(new S3Path(s3obj.key()));
			}
			
//			return list;
			return list.stream();
			
//    		return client.listObjectsV2Paginator(request).stream()
//    				.flatMap(response -> response.contents().stream())
//        			.map(S3Object::key)
//        			.map(S3Path::new);
    	}
    	
    	
    	
    	
    }
    
    
    //OK
    @Override public InputStream getInputStream(String key) throws IOException {
    	
    	String path = this.prefix + "/" + key;
    	
    	if(debug) {
    		System.out.println(getClass().getSimpleName() + ".getInputStream(" + path + ")");
    	}
    	
    	
//        final Path path = internalRoot.resolve(key);
//        if (Files.isReadable(path)) {
//            byte[] bytes = Files.readAllBytes(path);
//            return new ByteArrayInputStream(bytes);
//        }
//        return null;
    	
    	return this.cdse.getInputStream(path);
    }
    
    //OK
    @Override public OutputStream getOutputStream(String key) throws IOException {
    	
    	throw new UnsupportedOperationException();
    	
//        return new ByteArrayOutputStream() {
//            private boolean closed = false;
//
//            @Override
//            public void close() throws IOException {
//                try {
//                    if (!closed) {
//                        final byte[] bytes = this.toByteArray();
//                        final Path filePath = internalRoot.resolve(key);
//                        if (Files.exists(filePath)) {
//                            Files.delete(filePath);
//                        } else {
//                            Files.createDirectories(filePath.getParent());
//                        }
//                        Files.write(filePath, bytes);
//                    }
//                } finally {
//                    closed = true;
//                }
//            }
//        };
    	
    }
    
    //OK
    @Override public void delete(String key) throws IOException {
    	
    	throw new UnsupportedOperationException();
    	
//        final Path toBeDeleted = internalRoot.resolve(key);
//        if (Files.isDirectory(toBeDeleted)) {
//            ZarrUtils.deleteDirectoryTreeRecursively(toBeDeleted);
//        }
//        if (Files.exists(toBeDeleted)) {
//            Files.delete(toBeDeleted);
//        }
//        if (Files.exists(toBeDeleted) || Files.isDirectory(toBeDeleted)) {
//            throw new IOException("Unable to initialize " + toBeDeleted.toAbsolutePath().toString());
//        }
    }
    
    //OK
    @Override public TreeSet<String> getArrayKeys() throws IOException {
//        return getParentsOf(ZarrConstants.FILENAME_DOT_ZARRAY);
    	
    	if(debug) {
    		System.out.println(getClass().getSimpleName() + ".getArrayKeys()");
    	}
    	
    	TreeSet<String> set = getParentsOf(ZarrConstants.FILENAME_DOT_ZARRAY);
    	
    	if(debug) {
//    		System.out.println(getClass().getSimpleName() + ".getArrayKeys()");
    		set.forEach(element -> { System.out.println("\t" + element); });
    	}
    	
        return set;
    	
    }

    //OK
    @Override public TreeSet<String> getGroupKeys() throws IOException {
//        return getParentsOf(ZarrConstants.FILENAME_DOT_ZGROUP);
    	
    	if(debug) {
    		System.out.println(getClass().getSimpleName() + ".getGroupKeys()");
    	}
    	
    	
    	TreeSet<String> set = getParentsOf(ZarrConstants.FILENAME_DOT_ZGROUP);
    	
    	if(debug) {
//    		System.out.println(getClass().getSimpleName() + ".getGroupKeys()");
    		set.forEach(element -> { System.out.println("\t" + element); });
    	}
    	
        return set;
    	
    }

    //OK
    @Override public TreeSet<String> getKeysEndingWith(String suffix) throws IOException {
//        return Files.walk(internalRoot)
//                .filter(path -> path.toString().endsWith(suffix))
//                .map(path -> internalRoot.relativize(path).toString())
//                .collect(Collectors.toCollection(TreeSet::new));
    	
        if(debug) {
        	System.out.println(getClass().getSimpleName() + ".getKeysEndingWith(" + suffix + ")");
        }
        
        final S3Path suffixPath = new S3Path(suffix);
        try (Stream<S3Path> stream = getObjectList(keyPrefix.toString())) {
        	TreeSet<String> set = stream
              .filter(path -> path.endsWith(suffixPath))
              .map(S3Path::getParts)
              .map(parts -> new S3Path(parts.subList(keyPrefix.size(), parts.size())))
              .map(S3Path::toString)
              .collect(Collectors.toCollection(TreeSet::new));
        	
            if(debug) {
//            	System.out.println(getClass().getSimpleName() + ".getKeysEndingWith(" + suffix + ")");
            	set.forEach( element -> { System.out.println("\t" + element); });
            }
            
            return set;
        }
    	
    }
    
 	//OK
    @Override public Stream<String> getRelativeLeafKeys(String key) throws IOException {
//        final Path walkingRoot = internalRoot.resolve(key);
//        return Files.walk(walkingRoot)
//                .filter(path -> !Files.isDirectory(path))
//                .map(path -> walkingRoot.relativize(path).toString())
//                .map(ZarrUtils::normalizeStoragePath)
//                .filter(s -> s.trim().length() > 0);

    	if(debug) {
    		System.out.println(getClass().getSimpleName() + ".getRelativeLeafKeys(" + key + ")");

    		final S3Path rootPath = keyPrefix.resolve(key);
    		try (Stream<S3Path> stream = getObjectList(rootPath.toString())) {
    			TreeSet<String> keys = stream
    					.map(S3Path::getParts)
    					.map(parts -> new S3Path(parts.subList(rootPath.size(), parts.size())))
    					.filter(s3Path -> s3Path.size() > 0)
    					.map(S3Path::toString)
    					.collect(Collectors.toCollection(TreeSet::new));

    			keys.forEach( string -> { System.out.println("\t" + string); });	
    		}
        	 
        }
    	
    	
        final S3Path rootPath = keyPrefix.resolve(key);
        try (Stream<S3Path> stream = getObjectList(rootPath.toString())) {
        	TreeSet<String> keys = stream
              .map(S3Path::getParts)
              .map(parts -> new S3Path(parts.subList(rootPath.size(), parts.size())))
              .filter(s3Path -> s3Path.size() > 0)
              .map(S3Path::toString)
              .collect(Collectors.toCollection(TreeSet::new));
        	
        	
        	return keys.stream(); // wrap in collection and then stream as caller does not close the stream
        }
    }

}


