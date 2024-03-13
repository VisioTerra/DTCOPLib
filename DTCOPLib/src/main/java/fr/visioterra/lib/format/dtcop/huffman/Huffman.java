package fr.visioterra.lib.format.dtcop.huffman;

import java.util.LinkedHashMap;
import java.util.TreeMap;
import java.util.TreeSet;

import fr.visioterra.lib.io.bit.BitReader;
import fr.visioterra.lib.io.bit.BitWriter;

public class Huffman {
	
	private final LinkedHashMap<Integer, Node> symbols;
	private Node root = null;
	private int symbolCount = -1; 
	
	private Node merge(Node n1, Node n2, int s) {
		int w = n1.getWeight() + n2.getWeight();
		Node n = new Node(s, w, n1, n2);
		n1.incrDepth();
		n2.incrDepth();
		return n;
	}
	
	public Huffman() {
		this.symbols = new LinkedHashMap<>();
	}
	
	public void addSymbol(int symbol, int weight) {
		Node node = new Node(symbol, weight);
		
		if(symbols.containsKey(symbol)) {
			throw new IllegalArgumentException("Symbol \"" + symbol + "\" already set");
		}
		
		symbols.put(symbol, node);
	}
	
	public void buildTree() {
		
		if(this.root == null) {
			
			TreeSet<Node> set = new TreeSet<>(); 

			this.symbolCount = 0;
			for(Node node : this.symbols.values()) {
				set.add(node);
//				this.sortedMap.put(node.getSymbol(),node);
				this.symbolCount++;
			}

			int idx = Integer.MAX_VALUE;
			while(set.size() > 1) {

				Node n1 = set.pollFirst();
				Node n2 = set.pollFirst();
				Node n = merge(n1, n2, idx--);
				set.add(n);
			}

			this.root = set.first();
			this.root.updateCode(0);
		}
		
	}
	
	public void print() {
		
		TreeMap<Integer,Node> sortedMap = new TreeMap<>();
		for(Node node : this.symbols.values()) {
			sortedMap.put(node.getSymbol(),node);
		}
		for(Node node : sortedMap.values()) {
			System.out.println(node.getSymbol() + " : " + node.getWeight() + " / " + node.getDepth() + " / " + node.getCodeAsString());
		}
		
	}
	
	public int getSymbolCount() {
		return this.symbolCount;
	}

	public int getSymbolLength(int symbol) {
		
		Node node = this.symbols.get(symbol);
		if(node == null) {
			throw new IllegalArgumentException("symbol \"" + symbol + "\" not in alphabet");
		}
		
		return node.getDepth();
	}
	
	public String getCodeAsString(int symbol) {
		
		Node node = this.symbols.get(symbol);
		if(node == null) {
			throw new IllegalArgumentException("symbol \"" + symbol + "\" not in alphabet");
		}
		
		return node.getCodeAsString();
		
//		StringBuilder sb = new StringBuilder();
//		int len = node.getDepth();
//		int code = node.getCode();
//		
//		for(int i = len - 1 ; i >= 0 ; i--) {
//			
//			if( (code & (0x00000001 << i)) != 0) {
//				sb.append("1");
//			}
//			else {
//				sb.append("0");
//			}
//		}
//		
//		return sb.toString();
		
	}
	
	
	//write code
	public int writeCode(BitWriter bw, int symbol) throws Exception {
		
		Node node = this.symbols.get(symbol);
		if(node == null) {
			throw new IllegalArgumentException("symbol \"" + symbol + "\" not in alphabet");
		}
		
		return node.writeCode(bw);
	}
	
	//write code, n times
	public int writeCode(BitWriter bw, int symbol, int count) throws Exception {
		
		Node node = this.symbols.get(symbol);
		if(node == null) {
			throw new IllegalArgumentException("symbol \"" + symbol + "\" not in alphabet");
		}
		
		int sum = 0;
		for(int i = 0 ; i < count ; i++) {
			sum += node.writeCode(bw);
		}
		return sum;
	}
	
	
	//write table
	public int writeTable(BitWriter bw, int symbolLen) throws Exception {
		return this.root.writeNode(bw, symbolLen);
	}
	
	
	private Node readNode(BitReader br, int symbolCount, int symbolLen, int code, int depth) throws Exception {
		long b = br.readBits(1);
		
		if(b == 0) {
			Node left  = readNode(br, symbolCount, symbolLen, (code << 1) + 0, depth + 1);
			Node right = readNode(br, symbolCount, symbolLen, (code << 1) + 1, depth + 1);
			return new Node(Integer.MAX_VALUE, -1, left, right);
		}
		else {
			
			if(symbolLen != 16) {
				throw new IllegalArgumentException("Tuned for 16bits symbols");
			}
			
			int symbol = (short)br.readBits(symbolLen);
			this.symbolCount++;
			
			Node leaf  = new Node(symbol,-1);
			leaf.setDepth(depth);
			leaf.setCode(code);
			this.symbols.put(symbol,leaf);
			
			return leaf;
		}
		
	}
	
	//read table
	public Huffman readTable(BitReader br, int symbolCount, int symbolLen) throws Exception {
		this.symbolCount = 0;
		this.root = readNode(br, symbolCount, symbolLen, 0, 0);
		return this;
	}
	
	
	private static int readSymbol(Node node, BitReader br) throws Exception {
		
		if(node.isLeaf()) {
			return node.getSymbol();
		}
		else {
			long b = br.readBits(1);
			if(b == 0) {
				return readSymbol(node.getLeft(), br);
			}
			else {
				return readSymbol(node.getRight(),br);
			}
		}
		
	}
	
	//read symbol
	public int readSymbol(BitReader br) throws Exception {
		return readSymbol(this.root,br);
	}
	
}
