package fr.visioterra.lib.cache;

import java.util.Collection;
import java.util.EventListener;


/**
 * @author Grégory Mazabraud
 *
 * @param <K> key
 * @param <V> value
 * 
 * <br>Changes :
 * <br>- 2014/10/10		|	Grégory Mazabraud		|	initial version
 * <br>- 2014/10/12		|	Grégory Mazabraud		|	add event listener
 */
public interface KVStore<K,V> {
	
	public static class KVStoreEntry<V> {
		
		private final V value;
		private final long creationTime;
		private long lastAccessTime;
		private long hitCount;
		
		KVStoreEntry(V value, long creationTime, long lastAccessTime, long hitCount) {
			this.value = value;
			this.creationTime = creationTime;
			this.lastAccessTime = lastAccessTime;
			this.hitCount = hitCount;
		}
		
		public KVStoreEntry(V value) {
			this.value = value;
			this.creationTime = System.currentTimeMillis();
			this.lastAccessTime = System.currentTimeMillis();
			this.hitCount = 0;
		}

		public void hit() {
			this.lastAccessTime = System.currentTimeMillis();
			this.hitCount++;
		}
		
		public V getValue() {
			return this.value;
		}
		
		public long getCreationTime() {
			return this.creationTime;
		}
		
		public long getLastAccessTime() {
			return this.lastAccessTime;
		}
		
		public long getHitCount() {
			return this.hitCount;
		}
		
	}
	
	public interface KVStoreEventListener<K,V> extends EventListener {
		public void notifyEntryUpdated(KVStore<K,V> kvStore, K key, KVStoreEntry<V> entry);
		public void notifyEntryRemoved(KVStore<K,V> kvStore, K key, KVStoreEntry<V> entry);
		public void notifyEntryPut(    KVStore<K,V> kvStore, K key, KVStoreEntry<V> entry);
		public void notifyEntryEvicted(KVStore<K,V> kvStore, K key, KVStoreEntry<V> entry);
		public void notifyRemoveAll(   KVStore<K,V> kvStore);
	}
	
	public abstract class KVStoreEventAdapter<K,V> implements KVStoreEventListener<K,V> {
		@Override public void notifyEntryUpdated(KVStore<K,V> kvStore, K key, KVStoreEntry<V> entry) { }
		@Override public void notifyEntryRemoved(KVStore<K,V> kvStore, K key, KVStoreEntry<V> entry) { }
		@Override public void notifyEntryPut(    KVStore<K,V> kvStore, K key, KVStoreEntry<V> entry) { }
		@Override public void notifyEntryEvicted(KVStore<K,V> kvStore, K key, KVStoreEntry<V> entry) { }
		@Override public void notifyRemoveAll(   KVStore<K,V> kvStore) { }
	}
	
	
	/**
	 * @return true if the KVStore contains no item, false otherwise.
	 * @throws Exception if an error occurs.
	 */
	public boolean isEmpty() throws Exception;
	
	/**
	 * @return the approximative number of items in the KVStore.
	 * @throws Exception if an error occurs.
	 */
	public int size() throws Exception;
	
	/**
	 * remove all items in the KVStore. Call "notifyRemoveAll" on all KVStoreEventListener.
	 * @throws Exception if an error occurs.
	 */
	public void clear() throws Exception;
	
	/**
	 * @param key key
	 * @return true if the KVStore contains the key, false otherwise.
	 * @throws Exception if an error occurs.
	 */
	public boolean containsKey(K key) throws Exception;
	
	/**
	 * @return a collection of the keys contained in the KVStore. 
	 * @throws Exception if an error occurs.
	 */
	public Collection<K> keys() throws Exception;
	
	/**
	 * Get the value associate to a key. Call "notifyEntryUpdated" on all KVStoreEventListener.
	 * @param key key
	 * @return the value or null if the key does not exists or if no value is associates to the key.
	 * @throws Exception if an error occurs.
	 */
	public V get(K key) throws Exception;
	
	/**
	 * Remove the value associate to a key in the KVStore. Call "notifyEntryRemoved" on all KVStoreEventListener.
	 * @param key key
	 * @return the value or null if the key does not exists or if no value is associates to the key.
	 * @throws Exception if an error occurs.
	 */
	public V remove(K key) throws Exception;
	
	/**
	 * Put a key/value to the KVStore. Call "notifyEntryPut" on all KVStoreEventListener.
	 * If the key already existing and the value overwrite an old value, "notifyEntryEvicted" is called on all KVStoreEventListener.
	 * @param key
	 * @param value
	 * @throws Exception if an error occurs.
	 */
	public void put(K key, V value) throws Exception;
	
	/**
	 * Get the KVStoreEntry containing the value associates to the key. No event is thrown and no statistic is update.
	 * @param key
	 * @return the KVStoreEntry or null if the key does not exists.
	 * @throws Exception if an error occurs.
	 */
	public KVStoreEntry<V> getEntry(K key) throws Exception;
	
	/**
	 * Close all resources associates to the KVStore.
	 * @throws Exception if an error occurs.
	 */
	public void close() throws Exception;
	
	/**
	 * Indicates if the KVStore is able to keep values between two executions.
	 * @return true if values are persistent, false otherwise
	 */
	public boolean isPersistent();
	
	/**
	 * Indicates if the KVStore provides a eviction mechanism depending on a maximum size or number of entries.
	 * @return true if the KVStore works as a cache, false otherwise.
	 */
	public boolean isCache();
	
	/**
	 * Indicates if the KVStore supports event listener or not.
	 * @return true if event listener is supported or false otherwise.
	 */
	public boolean throwEvents();
	
	
	/**
	 * Add a listener to be notify for all events.
	 * @param listener the listener to be added.
	 */
	public void addKVStoreListener(KVStoreEventListener<K,V> listener);
	
	/**
	 * Remove a listener.
	 * @param listener the listener to be removed.
	 */
	public void removeKVStoreListener(KVStoreEventListener<K,V> listener);
	
	/**
	 * @return all the listeners.
	 */
	public KVStoreEventListener<K,V>[] getKVStoreListeners();
	
	/**
	 * @return the number of listeners.
	 */
	public int getKVStoreListenerCount();

}
