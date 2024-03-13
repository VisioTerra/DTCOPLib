package fr.visioterra.lib.format.dtcop.huffman;

import fr.visioterra.lib.io.bit.BitWriter;

public class Node implements Comparable<Node> {
	
	private final int symbol;
	private final int weight;
	private final Node left;
	private final Node right;
	private int depth = 0;
	private int code = 0;
	
	public Node(int symbol, int weight) {
		this(symbol,weight,null,null);
	}
	
	public Node(int symbol, int weight, Node left, Node right) {
		
		if(left == null) {
			if(right != null) {	throw new IllegalArgumentException();	}
		} else {
			if(right == null) {	throw new IllegalArgumentException();	}				
		}
		
//		System.out.println("Node(" + symbol + "," + weight + "," + left + "," + right + ")");
		this.symbol = symbol;
		this.weight = weight;
		this.left   = left;
		this.right  = right;
	}

	public int getSymbol() {
		return this.symbol;
	}
	
	public int getWeight() {
		return this.weight;
	}
	
	public boolean isLeaf() {
		return this.left == null && this.right == null;
	}
	
	public Node getLeft() {
		return this.left;
	}
	
	public Node getRight() {
		return this.right;
	}
	
	public int getDepth() {
		return this.depth;
	}
	
	public void setDepth(int depth) {
		this.depth = depth;
	}
	
	public int getCode() {
		return this.code;
	}
	
	public void setCode(int code) {
		this.code = code;
	}
	
	public void incrDepth() {
		this.depth++;
		
		if(this.left != null) {
			this.left.incrDepth();
		}
		
		if(this.right != null) {
			this.right.incrDepth();
		}
		
	}
	
	public void updateCode(int code) {
		
		this.code = code;
		
		if(this.left != null) {
			this.left.updateCode((code << 1) + 0);
		}
		
		if(this.right != null) {
			this.right.updateCode((code << 1) + 1);
		}
	}
	
	public String getCodeAsString() {
		StringBuilder sb = new StringBuilder();
		
		for(int i = this.depth - 1 ; i >= 0 ; i--) {
			
			if( (this.code & (0x00000001 << i)) != 0) {
				sb.append("1");
			}
			else {
				sb.append("0");
			}
		}
		
		return sb.toString();
	}
	
	@Override public boolean equals(Object obj) {
		if(obj instanceof Node == false) {
			return false;
		}
		else {
			return this.symbol == ((Node)obj).symbol;
		}
	}
	
	@Override public int hashCode() {
		return symbol;
	}
	
	@Override public int compareTo(Node that) {
		
		if(this.weight != that.weight) {
			return Integer.compare(this.weight, that.weight);	
		}
		else {
			return Integer.compare(this.symbol, that.symbol);
		}
		
	}
	
	public int writeCode(BitWriter bw) throws Exception {
		bw.writeBits(this.code,this.depth);
		return this.depth;
	}
	
	public int writeNode(BitWriter bw, int symbolLen) throws Exception {
		
		if(this.left == null && this.right == null) {
			bw.writeBits(1,1);
			bw.writeBits(getSymbol(),symbolLen);
			return 1 + symbolLen;
		}
		else {
			int sum = 1;
			bw.writeBits(0,1);
			sum += this.left.writeNode(bw, symbolLen);
			sum += this.right.writeNode(bw, symbolLen);
			return sum;
		}
		
	}
	
}
