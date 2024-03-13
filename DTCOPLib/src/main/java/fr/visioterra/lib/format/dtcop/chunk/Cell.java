package fr.visioterra.lib.format.dtcop.chunk;

import java.util.TreeSet;

public class Cell implements Comparable<Cell> {

	private final int[] coordinates;
	private final boolean zigzag;
	
	private int sum() {
		int sum = 0;
		for(int c : coordinates) {
			sum += c;
		}
		return sum;
	}
	
	private int sumsqr() {
		int sum = 0;
		for(int c : coordinates) {
			sum += c * c;
		}
		return sum;
	}
	
	public Cell(int[] coordinates) {
		this(coordinates,true);
	}
	
	public Cell(int[] coordinates, boolean zigzag) {
		this.coordinates = coordinates;
		this.zigzag = zigzag;
	}
	
	public int[] getCoordinates() {
		return this.coordinates;
	}

	@Override public boolean equals(Object o) {
		
		if(o instanceof Cell == false) {
			return false;
		}
		
		Cell c = (Cell)o;
			
		if(this.coordinates.length != c.coordinates.length) {
			return false;
		}
		
		for(int i = 0 ; i < this.coordinates.length ; i++) {
			int c1 = this.coordinates[i];
			int c2 = c.coordinates[i];
			
			if(c1 != c2) {
				return false;
			}
		}
		
		return true;
	}
	
	@Override public int compareTo(Cell that) {
		
		if(this.coordinates.length != that.coordinates.length) {
			throw new IllegalArgumentException("coordinates lengths mismatch (" + this.coordinates.length + "!=" + that.coordinates.length + ")");
		}
		
		if(this.zigzag) {
			
			//check this.sum() == that.sum()
			int sum1 = this.sum();
			int sum2 = that.sum();
			if(sum1 > sum2) {
				return 1;
			}
			else if(sum2 > sum1) {
				return -1;
			}

			//check this.sumsqr() == that.sumsqr()			
			sum1 = this.sumsqr();
			sum2 = that.sumsqr();
			if(sum1 > sum2) {
				return 1;
			}
			else if(sum2 > sum1) {
				return -1;
			}
		}

		//compare individual coordinates
		for(int i = 0 ; i < this.coordinates.length ; i++) {

			int c1 = this.coordinates[i];
			int c2 = that.coordinates[i];

			if(c1 > c2) {
				return 1;
			}
			else if(c2 > c1) {
				return -1;
			}

		}

		return 0;
	}
	
	@Override public String toString() {
		StringBuilder sb = new StringBuilder();
		boolean first = true;
		for(int c : coordinates) {
			if(first == false) {
				sb.append(",");
			}
			first = false;
			sb.append(c);
		}
		return sb.toString();
	}

	private static TreeSet<Cell> order(int[] shape, int[] idxCoords, int dim, TreeSet<Cell> cells, boolean zigzag) {

		if(dim >= idxCoords.length) {
			cells.add(new Cell(idxCoords.clone(),zigzag));
		}
		else {
			int len = shape[dim];
			for(int i = 0 ; i < len ; i++) {
				idxCoords[dim] = i;
				order(shape,idxCoords,dim+1,cells,zigzag);
			}
		}
		
		return cells;
	}
	
	public static Cell[] order(int[] shape, boolean zigzag) {
		TreeSet<Cell> cells = new TreeSet<>();
		order(shape,new int[shape.length],0,cells,zigzag);
		return cells.toArray(new Cell[cells.size()]);
	}
	
}
