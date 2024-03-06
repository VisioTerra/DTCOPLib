package fr.visioterra.lib.cache;

import java.net.URL;
import java.util.Collection;
import java.util.HashMap;

import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheException;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Ehcache;
import net.sf.ehcache.Element;
import net.sf.ehcache.config.CacheConfiguration;
import net.sf.ehcache.config.MemoryUnit;
import net.sf.ehcache.config.PersistenceConfiguration;
import net.sf.ehcache.config.PersistenceConfiguration.Strategy;
import net.sf.ehcache.event.CacheEventListener;
import net.sf.ehcache.store.MemoryStoreEvictionPolicy;


@SuppressWarnings("unchecked")
public class KVCacheEhcache<K, V> implements KVStore<K, V> {

	private final Ehcache cache;
	private final HashMap<KVStoreEventListener<K, V>,CacheEventListener> map = new HashMap<KVStoreEventListener<K,V>, CacheEventListener>();
	private final boolean throwEvents;
	
	private K elementToKey(Element e) {
		return (K)e.getObjectKey();
	}
	
	private KVStoreEntry<V> elementToKVStoreEntry(Element e) {
		return new KVStoreEntry<V>((V)e.getObjectValue(),e.getCreationTime(),e.getLastAccessTime(),e.getHitCount());
	}
	
	private KVCacheEhcache(CacheManager cacheManager, CacheConfiguration cacheConfiguration, boolean throwEvents) {
		Ehcache tmpCache = new Cache(cacheConfiguration);
		cacheManager.addCache(tmpCache);
		this.cache = tmpCache;
		this.throwEvents = throwEvents;
	}
	
	private KVCacheEhcache(CacheManager cacheManager, String cacheName, boolean throwEvents) {
		cacheManager.addCache(cacheName);
		this.cache = cacheManager.getCache(cacheName);
		this.throwEvents = throwEvents;
	}

	public void registerListener(CacheEventListener listener) {
		this.cache.getCacheEventNotificationService().registerListener(listener);
	}

	public void unregisterListener(CacheEventListener listener) {
		this.cache.getCacheEventNotificationService().unregisterListener(listener);
	}
	
	@Override public boolean isEmpty() throws Exception {
		return this.size() == 0;
	}

	@Override public int size() throws Exception {
		return this.cache.getSize();
		//return this.cache.getKeysWithExpiryCheck().size();
	}

	@Override public void clear() throws Exception {
		this.cache.removeAll();
	}

	@Override public boolean containsKey(K key) throws Exception {
		return this.cache.getQuiet(key) != null;
	}

	@Override public Collection<K> keys() throws Exception {
		return this.cache.getKeysWithExpiryCheck();
	}

	@Override public V get(K key) throws Exception {
		Element e = this.cache.get(key);
		if (e == null) return null;
		return (V) e.getObjectValue();
	}

	@Override public V remove(K key) throws Exception {
		V tmp = this.get(key);
		this.cache.remove(key);
		return tmp;
	}

	@Override public void put(K key, V value) throws Exception {
		this.cache.put(new Element(key, value));
	}

	@Override public KVStoreEntry<V> getEntry(K key) throws Exception {
		Element e = this.cache.getQuiet(key);
		if(e == null) {
			return null;
		} else {
			return elementToKVStoreEntry(e);
		}
	}
	
	@Override public void close() throws Exception {
		for (K key : this.keys()) {
			V value = this.get(key);
			if (value instanceof AutoCloseable) {
				((AutoCloseable) value).close();
			}

			if (key instanceof AutoCloseable) {
				((AutoCloseable) key).close();
			}
		}
	}

	@Override public boolean isPersistent() {
		return false;
	}

	@Override public boolean isCache() {
		return true;
	}

	@Override public boolean throwEvents() {
		return this.throwEvents;
	}
	
	@Override public void addKVStoreListener(final KVStoreEventListener<K,V> listener) {
		if(throwEvents) {
			CacheEventListener cel = new CacheEventListener() {

				@Override public void notifyRemoveAll(Ehcache cache) {
					listener.notifyRemoveAll(KVCacheEhcache.this);
				}

				@Override public void notifyElementUpdated(Ehcache cache, Element e) throws CacheException {
					listener.notifyEntryUpdated(KVCacheEhcache.this,elementToKey(e), elementToKVStoreEntry(e));
				}

				@Override public void notifyElementRemoved(Ehcache cache, Element e) throws CacheException {
					listener.notifyEntryRemoved(KVCacheEhcache.this,elementToKey(e),elementToKVStoreEntry(e));
				}

				@Override public void notifyElementPut(Ehcache cache, Element e) throws CacheException {
					listener.notifyEntryPut(KVCacheEhcache.this,elementToKey(e),elementToKVStoreEntry(e));
				}

				@Override public void notifyElementExpired(Ehcache cache, Element e) {
					//no equivalence in KVStoreEventListener
				}

				@Override public void notifyElementEvicted(Ehcache cache, Element e) {
					listener.notifyEntryEvicted(KVCacheEhcache.this,elementToKey(e),elementToKVStoreEntry(e));
				}

				@Override public void dispose() {
					//no equivalence in KVStoreEventListener
				}

				@Override public Object clone() throws CloneNotSupportedException {
					throw new CloneNotSupportedException();
				};
			};
			this.cache.getCacheEventNotificationService().registerListener(cel);
			this.map.put(listener, cel);
		}
	}
	
	@Override public void removeKVStoreListener(KVStoreEventListener<K,V> listener) {
		if(throwEvents) {
			CacheEventListener cel = map.remove(listener);
			if(cel != null) {
				this.cache.getCacheEventNotificationService().unregisterListener(cel);
			}
		}
	}
	
	@Override public KVStoreEventListener<K,V>[] getKVStoreListeners() {
		return this.map.keySet().toArray(new KVStoreEventListener[0]);
	}
	
	@Override public int getKVStoreListenerCount() {
		return this.map.size();
	}
	
	public static <K, V> KVCacheEhcache<K, V> createCache(CacheConfiguration cacheConfiguration, boolean throwEvents) {
		return new KVCacheEhcache<>(CacheManager.getInstance(), cacheConfiguration,throwEvents);
	}
	
	/**
	 * @param cacheName
	 * @param maxElementsInMemory The maximum number of elements in memory, before they are evicted (0 == no limit)
	 * @param timeToIdleSeconds the default amount of time to live for an element from its last accessed or modified date (0 == infinite lifetime)
	 * @return an EhcacheSimpleKeyValueCache object
	 */
	public static <K, V> KVCacheEhcache<K, V> createMemoryCache(String cacheName, int maxElementsInMemory, long timeToIdleSeconds, boolean throwEvents) {
		return new KVCacheEhcache<>(CacheManager.getInstance(), new CacheConfiguration()
			.name(cacheName)
			.maxEntriesLocalHeap(maxElementsInMemory)
			.memoryStoreEvictionPolicy(MemoryStoreEvictionPolicy.LRU)
			.timeToIdleSeconds(timeToIdleSeconds), throwEvents);
	}

	/**
	 * @param cacheName
	 * @param maxBytesLocalHeap String representation of the size, use the Java -Xmx syntax (for example: "500k", "200m", "2g")
	 * @param maxBytesLocalDisk String representation of the size, use the Java -Xmx syntax (for example: "500k", "200m", "2g")
	 * @param timeToIdleSeconds the default amount of time to live for an element from its last accessed or modified date (0 == infinite lifetime)
	 * @return an EhcacheSimpleKeyValueCache object
	 */
	public static <K, V> KVCacheEhcache<K, V> createMemoryAndDiskCache(String cacheName, String maxBytesLocalHeap, String maxBytesLocalDisk, long timeToIdleSeconds, boolean throwEvents) {
		CacheConfiguration cacheConfiguration = new CacheConfiguration()
			.name(cacheName)
			.memoryStoreEvictionPolicy(MemoryStoreEvictionPolicy.LRU)
			.timeToIdleSeconds(timeToIdleSeconds)
			.persistence(new PersistenceConfiguration().strategy(Strategy.LOCALTEMPSWAP));
		
		cacheConfiguration.setMaxBytesLocalHeap(maxBytesLocalHeap);
		cacheConfiguration.setMaxBytesLocalDisk(maxBytesLocalDisk);

		return new KVCacheEhcache<>(CacheManager.getInstance(), cacheConfiguration,throwEvents);
	}
	
	public static <K, V> KVCacheEhcache<K, V> createMemoryCache(String cacheName, long amount, MemoryUnit memoryUnit, long timeToIdleSeconds, boolean throwEvents) {
		CacheConfiguration cacheConfiguration = new CacheConfiguration()
				.name(cacheName)
				.maxBytesLocalHeap(amount, memoryUnit)
				.memoryStoreEvictionPolicy(MemoryStoreEvictionPolicy.LRU)
				.timeToIdleSeconds(timeToIdleSeconds)
				.persistence(new PersistenceConfiguration().strategy(Strategy.LOCALTEMPSWAP));
		
		return new KVCacheEhcache<>(CacheManager.getInstance(), cacheConfiguration,throwEvents);
	}
	
	public static <K, V> KVCacheEhcache<K, V> createMemoryAndDiskCache(String configFileName, String cacheName, boolean throwEvents) {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        URL url = classLoader.getResource("/" + configFileName); // get url from classpath root 
        
		CacheManager cacheManager = CacheManager.create(url);
		return new KVCacheEhcache<>(cacheManager,cacheName,throwEvents);
	}
	
}
