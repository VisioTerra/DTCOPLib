package fr.visioterra.lib.cache;

import javax.swing.event.EventListenerList;

/**
 * @KVStoreAdapter provide a default implementation of KVStoreEventListener support
 * @author Grégory Mazabraud
 * 
 * <br>Changes :
 * <br>- 2014/10/12		|	Grégory Mazabraud		|	initial version
 */
public abstract class KVStoreAdapter<K,V> implements KVStore<K,V> {

	private final EventListenerList listeners = new EventListenerList();
	private final boolean throwEvents;
	
	public KVStoreAdapter(boolean throwEvents) {
		this.throwEvents = throwEvents;
	}
	
	void fireEntryUpdated(K key, KVStoreEntry<V> entry) {
		if(throwEvents) {
			for(KVStoreEventListener<K, V> listener : getKVStoreListeners()) {
				listener.notifyEntryUpdated(this,key,entry);
			}
		}
	}
	
	void fireEntryRemoved(K key, KVStoreEntry<V> entry) {
		if(throwEvents) {
			for(KVStoreEventListener<K, V> listener : getKVStoreListeners()) {
				listener.notifyEntryRemoved(this,key,entry);
			}
		}
	}

	void fireEntryPut(K key, KVStoreEntry<V> entry) {
		if(throwEvents) {
			for(KVStoreEventListener<K, V> listener : getKVStoreListeners()) {
				listener.notifyEntryPut(this,key,entry);
			}
		}
	}

	void fireEntryEvicted(K key, KVStoreEntry<V> entry) {
		if(throwEvents) {
			for(KVStoreEventListener<K, V> listener : getKVStoreListeners()) {
				listener.notifyEntryEvicted(this,key,entry);
			}
		}
	}
	
	void fireRemoveAll() {
		if(throwEvents) {
			for(KVStoreEventListener<K, V> listener : getKVStoreListeners()) {
				listener.notifyRemoveAll(this);
			}
		}
	}
	
	@Override public boolean throwEvents() {
		return this.throwEvents;
	}
	
	@Override public void addKVStoreListener(KVStoreEventListener<K,V> listener) {
		if(throwEvents) {
			listeners.add(KVStoreEventListener.class, listener);
		}
	}
	
	@Override public void removeKVStoreListener(KVStoreEventListener<K,V> listener) {
		if(throwEvents) {
			listeners.remove(KVStoreEventListener.class, listener);
		}
	}
	
	@SuppressWarnings("unchecked")
	@Override public KVStoreEventListener<K,V>[] getKVStoreListeners() {
		return listeners.getListeners(KVStoreEventListener.class);
	}
	
	@Override public int getKVStoreListenerCount() {
		return listeners.getListenerCount(KVStoreEventListener.class);
	}
	
}
