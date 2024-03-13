package fr.visioterra.lib.format.dtcop.huffman;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map.Entry;

public class Histogram {

	private final Object lock = new Object();		//lock to protect update operations 
	private final int[] histo;						//for regular values
	private final int offset;						//offset to map signed values to int array
	private final HashMap<Integer, Integer> map;	//map for values that cannot be handle by int array
	private int symbolCount = 0;					//number of symbols
	private long totalCount = 0;					//total number of samples
 
	
	public Histogram(int size, int offset) {
		this.histo = new int[size];
		this.offset = offset;
		this.map = new HashMap<>();
	}
	
	public void update(int sample) {
		update(sample,1);
	}
	
	public void update(int sample, int occurence) {
		
		int idx = this.offset + sample;
		
		if(0 < idx && idx < histo.length) {
			synchronized (this.lock) {
				if(this.histo[idx] == 0) {
					this.symbolCount++;
				}
				this.histo[idx] += occurence;
				this.totalCount += occurence;
			}
		}
		else {
			synchronized (this.lock) {
				Integer k = new Integer(sample);
				Integer v = this.map.get(k);
				if(v == null) {
					this.symbolCount++;
					this.map.put(k,new Integer(occurence));
				}
				else {
					this.map.put(k,new Integer(v.intValue() + occurence));
				}
				this.totalCount += occurence;
			}
		}
		
		
	}
	
	public int getSymbolCount() {
		synchronized (this.lock) {
			return this.symbolCount;	
		}
	}
	
	public long getTotalCount() {
		synchronized (this.lock) {
			return this.totalCount;
		}
	}
	
	@Override public String toString() {
		synchronized (this.lock) {
			String keys = Arrays.toString(map.keySet().toArray(new Integer[0]));
			return "totalCount = " + this.totalCount + " / map.size() = " + this.map.size() + " / " + keys;
		}
	}
	
	public Huffman getHuffman() {
		
		Huffman huffman = new Huffman();
		
		synchronized (this.lock) {

			for(int symbol = 0 ; symbol < this.histo.length ; symbol++) {
				int weight = this.histo[symbol];
				if(weight > 0) {
					huffman.addSymbol(symbol - this.offset, weight);
				}
			}

			for (Entry<Integer, Integer> entry : this.map.entrySet()) {
				int symbol = entry.getKey().intValue(); // - this.offset;
				int weight = entry.getValue().intValue();
				if(weight > 0) {
					huffman.addSymbol(symbol, weight);
					//				System.out.println("huffman.addSymbol(" + symbol + "," + weight + ")");
				}
			}
			
		}
		
		huffman.buildTree();
		return huffman;
	}
	
}
