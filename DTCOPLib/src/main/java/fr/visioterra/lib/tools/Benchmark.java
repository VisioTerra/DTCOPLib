package fr.visioterra.lib.tools;

public class Benchmark {
	
	private final String message;
	private long start = 0;
	private long sum = 0;
	private int call = 0;
	
	public Benchmark() {
		this.message = null;
		reset();
	}
	
	public Benchmark(String message) {
		this.message = message;
		reset();
	}
	
	public void reset() {
		this.start = 0;
		this.sum = 0;
		this.call = 0;
	}
	
	public void start() {
		if(start != 0) {
			throw new IllegalStateException();
		}
		this.start = System.nanoTime();
	}
	
	public void stop() {
		if(start == 0) {
			throw new IllegalStateException();
		}
		this.sum += (System.nanoTime() - this.start);
		this.start = 0;
		this.call++;
	}
	
	public long getTotalDuration() {
		return this.sum / 1_000_000L;
	}
	
	public int getCallNumber() {
		return this.call;
	}
	
	@Override public String toString() {

		StringBuilder sb = new StringBuilder();
		
		if(this.message != null) {
			sb.append(message);
			sb.append(" : ");
		}
		
		sb.append("nb call = ").append(this.call);
		sb.append(" / total time = ").append(this.sum / 1_000_000L).append(" ms");
		sb.append(" / time per call = ").append(this.sum / 1_000 / this.call).append(" µs/call");
		
		return sb.toString();
	}
	
}
