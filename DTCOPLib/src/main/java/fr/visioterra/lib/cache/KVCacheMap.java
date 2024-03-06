package fr.visioterra.lib.cache;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * @KVCacheMap implementation of @KVStore with a @HashMap and eviction mechanism 
 * @author Grégory Mazabraud
 *
 * <br>Changes :
 * <br>- 2014/10/10		|	Grégory Mazabraud		|	initial version
 * <br>- 2014/10/12		|	Grégory Mazabraud		|	add event listener
 */
public class KVCacheMap<K,V> extends KVStoreAdapter<K, V> {
	
	private final Object lock;
	private final HashMap<K,KVStoreEntry<V>> map;
	private final int maxSize;
	private final int removeSize;
	
	/**
	 * 
	 * @param throwEvents
	 * @param maxSize maximum number of object in the cache
	 * @throws IllegalArgumentException
	 */
	public KVCacheMap(boolean throwEvents, int maxSize) throws IllegalArgumentException {
		super(throwEvents);
		this.lock = new Object();
		this.map = new HashMap<K,KVStoreEntry<V>>();
		this.maxSize = maxSize;
		this.removeSize = Math.max(maxSize / 4 , 1);
	}
	
	@Override public boolean isEmpty() {
		synchronized(this.lock) {
			return this.map.isEmpty();
		}
	}
	
	@Override public int size() {
		synchronized(this.lock) {
			return this.map.size();
		}
	}
	
	@Override public void clear() {
		synchronized(this.lock) {
			fireRemoveAll();
			this.map.clear();
		}
	}
	
	@Override public boolean containsKey(K key) {
		synchronized(this.lock) {
			return this.map.containsKey(key);
		}
	}
	
	@Override public Collection<K> keys() {
		synchronized(this.lock) {
			return this.map.keySet();
		}
	}
	
	@Override public V get(K key) {
		synchronized(this.lock) {
			KVStoreEntry<V> entry = this.map.get(key);
			if(entry != null) {
				entry.hit();
				fireEntryUpdated(key, entry);
				return entry.getValue();
			} else {
				return null;
			}
		}
	}
	
	@Override public V remove(K key) {
		synchronized(this.lock) {
			KVStoreEntry<V> entry = this.map.remove(key);
			if(entry != null) {
				fireEntryRemoved(key, entry);
				return entry.getValue();
			} else {
				return null;
			}
		}
	}
	
	@Override public void put(K key, V value) {
		synchronized(this.lock) {
			
			KVStoreEntry<V> removed = this.map.remove(key);
			if(removed != null) {
				fireEntryEvicted(key, removed);
			}
			
			//check map size
			if(this.map.size() >= maxSize) {

				//remove keys/values with the older access time
				TreeMap<Long,List<Map.Entry<K,KVStoreEntry<V>>>> treeMap = new TreeMap<>();
				int count = 0;

				//build the list of items to evict
				for(Map.Entry<K,KVStoreEntry<V>> entry : this.map.entrySet()) {

					Long currentKey = entry.getValue().getLastAccessTime();
					Long lastKey = treeMap.size() == 0 ? currentKey+1 : treeMap.lastKey();

					if(lastKey > currentKey && count >= this.removeSize) {
						List<Map.Entry<K,KVStoreEntry<V>>> list = treeMap.remove(lastKey);
						if(list.size() > 1) {
							list.remove(0);
							treeMap.put(lastKey,list);
						}
						count--;
					}

					if(lastKey > currentKey || count < this.removeSize) {
						List<Map.Entry<K,KVStoreEntry<V>>> list = treeMap.get(currentKey);
						if(list == null) {
							list = new LinkedList<>();
							treeMap.put(currentKey,list);
						}
						list.add(entry);
						count++;
					}
				}

				//evict the items of the list
				for(Long currentTimeStamp : treeMap.keySet()) {
					List<Map.Entry<K,KVStoreEntry<V>>> list = treeMap.get(currentTimeStamp);
					for(Map.Entry<K,KVStoreEntry<V>> entry : list) {
						this.map.remove(entry.getKey());
						fireEntryEvicted(key, entry.getValue());
					}
				}
					
			}
			this.map.put(key, new KVStoreEntry<V>(value));
		}
	}
	
	@Override public KVStoreEntry<V> getEntry(K key) {
		synchronized(this.lock) {
			return this.map.get(key);
		}
	}
	
	@Override public void close() {
		this.clear();
	}

	@Override public boolean isPersistent() {
		return false;
	}
	
	@Override public boolean isCache() {
		return true;
	}
	
}
