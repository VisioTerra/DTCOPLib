/*
 *
 * MIT License
 *
 * Copyright (c) 2020. Brockmann Consult GmbH (info@brockmann-consult.de)
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */

package com.bc.zarr.storage;

import com.bc.zarr.ZarrConstants;
import com.bc.zarr.ZarrUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FileSystemStore implements Store {

	public static boolean debug = false;
	public static int readVersion = 0;
	public static int writeVersion = 0;
	public static int writeBufferSize = 8 * 1024;
    private final Path internalRoot;

    public FileSystemStore(String path, FileSystem fileSystem) {
        if (fileSystem == null) {
            internalRoot = Paths.get(path);
        } else {
            internalRoot = fileSystem.getPath(path);
        }
    }

    public FileSystemStore(Path rootPath) {
        internalRoot = rootPath;
    }
    
    
    public Path getInternalRoot() {
    	return internalRoot;
    }
    
    public File getFile(String key) {
    	return internalRoot.resolve(key).toFile();
    }

    private InputStream getInputStream0(String key) throws IOException {
        final Path path = internalRoot.resolve(key);
        if (Files.isReadable(path)) {
            byte[] bytes = Files.readAllBytes(path);
            return new ByteArrayInputStream(bytes);
        }
        return null;
    }
    
    private InputStream getInputStream1(String key) throws IOException {
        final Path path = internalRoot.resolve(key);
        if (Files.isReadable(path)) {
            byte[] bytes = Files.readAllBytes(path);
            return new fr.visioterra.lib.io.stream.ByteArrayInputStream(bytes);
        }
        return null;
    }
    
    private InputStream getInputStream2(String key) throws IOException {
        final Path path = internalRoot.resolve(key);
        File file = internalRoot.resolve(key).toFile();
        
        byte[] array = new byte[(int)file.length()];
        try(RandomAccessFile raf = new RandomAccessFile(path.toFile(),"r")) {
        	raf.readFully(array);
        }
        return new fr.visioterra.lib.io.stream.ByteArrayInputStream(array);
        
    }
    
    private InputStream getInputStream3(String key) throws IOException {
//        final Path path = internalRoot.resolve(key);
        File file = internalRoot.resolve(key).toFile();
        return new FileInputStream(file);
    }
    
    //debug
    @Override public InputStream getInputStream(String key) throws IOException {
    	
    	if(debug) {
    		System.out.println(getClass().getSimpleName() + ".getInputStream(" + key + ")");
    	}
    	
    	if(readVersion == 0) {
    		return getInputStream0(key);
    	}
    	else if(readVersion == 1) {
    		return getInputStream1(key);
    	}
    	else if(readVersion == 2) {
    		return getInputStream2(key);
    	}
    	else if(readVersion == 3) {
    		return getInputStream3(key);
    	}
    	else {
    		throw new IOException("Invalid FileSystemStore.version value (" + readVersion + ")");
    	}
    	
    }
    
    private OutputStream getOutputStream0(String key) {
        return new ByteArrayOutputStream() {
            private boolean closed = false;

            @Override
            public void close() throws IOException {
                try {
                    if (!closed) {
                        final byte[] bytes = this.toByteArray();
                        final Path filePath = internalRoot.resolve(key);
                        if (Files.exists(filePath)) {
                            Files.delete(filePath);
                        } else {
                            Files.createDirectories(filePath.getParent());
                        }
                        Files.write(filePath, bytes);
                    }
                } finally {
                    closed = true;
                }
            }
        };
    }
    
    private OutputStream getOutputStream1(String key) {
    	
        return new fr.visioterra.lib.io.stream.ByteArrayOutputStream(writeBufferSize) {
            private boolean closed = false;

            @Override public void close() throws IOException {
                try {
                    if (!closed) {
                        final byte[] bytes = this.toByteArray();
                        final Path filePath = internalRoot.resolve(key);
                        if (Files.exists(filePath)) {
                            Files.delete(filePath);
                        } else {
                            Files.createDirectories(filePath.getParent());
                        }
                        Files.write(filePath, bytes);
                    }
                } finally {
                    closed = true;
                }
            }
        };
    }
    
    @Override public OutputStream getOutputStream(String key) throws IOException {
    	if(writeVersion == 0) {
    		return getOutputStream0(key);
    	}
    	else if(writeVersion == 1) {
    		return getOutputStream1(key);
    	}
    	else {
    		throw new IOException("Invalid FileSystemStore.version value (" + writeVersion + ")");
    	}
    	
    }
    
    @Override public void delete(String key) throws IOException {
        final Path toBeDeleted = internalRoot.resolve(key);
        if (Files.isDirectory(toBeDeleted)) {
            ZarrUtils.deleteDirectoryTreeRecursively(toBeDeleted);
        }
        if (Files.exists(toBeDeleted)) {
            Files.delete(toBeDeleted);
        }
        if (Files.exists(toBeDeleted) || Files.isDirectory(toBeDeleted)) {
            throw new IOException("Unable to initialize " + toBeDeleted.toAbsolutePath().toString());
        }
    }

    private TreeSet<String> getParentsOf(String suffix) throws IOException {
    	
    	if(debug) {
    		System.out.println(getClass().getSimpleName() + ".getParentsOf(" + suffix + ")");
    	}
    	
        return getKeysEndingWith(suffix).stream()
                .map(s -> internalRoot.relativize(internalRoot.resolve(s).getParent()).toString())
                .collect(Collectors.toCollection(TreeSet::new));
    }
    
    //debug
    @Override public TreeSet<String> getArrayKeys() throws IOException {
    	
//    	return getParentsOf(ZarrConstants.FILENAME_DOT_ZARRAY);
    	
    	if(debug) {
    		System.out.println(getClass().getSimpleName() + ".getArrayKeys()");
    	}
    	
    	TreeSet<String> set = getParentsOf(ZarrConstants.FILENAME_DOT_ZARRAY);
    	
    	if(debug) {
    		System.out.println(getClass().getSimpleName() + ".getArrayKeys()");
    		set.forEach(element -> { System.out.println("\t" + element); });
    	}
    	
        return set;
    }

    //debug
    @Override public TreeSet<String> getGroupKeys() throws IOException {
    	
//    	return getParentsOf(ZarrConstants.FILENAME_DOT_ZGROUP);
    	
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

    //debug
    @Override public TreeSet<String> getKeysEndingWith(String suffix) throws IOException {
//        return Files.walk(internalRoot)
//                .filter(path -> path.toString().endsWith(suffix))
//                .map(path -> internalRoot.relativize(path).toString())
//                .collect(Collectors.toCollection(TreeSet::new));
    	
        if(debug) {
        	System.out.println(getClass().getSimpleName() + ".getKeysEndingWith(" + suffix + ")");
        }
        
        TreeSet<String> set = Files.walk(internalRoot)
                .filter(path -> path.toString().endsWith(suffix))
                .map(path -> internalRoot.relativize(path).toString())
                .collect(Collectors.toCollection(TreeSet::new));
        
        if(debug) {
//        	System.out.println(getClass().getSimpleName() + ".getKeysEndingWith(" + suffix + ")");
        	set.forEach( element -> { System.out.println("\t*" + element); });
        }
        
        return set;
    }

    //debug
    @Override public Stream<String> getRelativeLeafKeys(String key) throws IOException {
    	
    	if(debug) {
        	System.out.println(getClass().getSimpleName() + ".getRelativeLeafKeys(" + key + ")");
        	
        	 final Path walkingRoot = internalRoot.resolve(key);
        	 Stream<String> stream = Files.walk(walkingRoot)
                     .filter(path -> !Files.isDirectory(path))
                     .map(path -> walkingRoot.relativize(path).toString())
                     .map(ZarrUtils::normalizeStoragePath)
                     .filter(s -> s.trim().length() > 0);
        	 
        	 stream.forEach( string -> { System.out.println("\t" + string); });
        }
    	
        final Path walkingRoot = internalRoot.resolve(key);
        return Files.walk(walkingRoot)
                .filter(path -> !Files.isDirectory(path))
                .map(path -> walkingRoot.relativize(path).toString())
                .map(ZarrUtils::normalizeStoragePath)
                .filter(s -> s.trim().length() > 0);
    }
    
}
