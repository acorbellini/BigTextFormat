package edu.bigtextformat;

public class Range {

	private byte[] first;
	private byte[] last;

	public Range(byte[] first, byte[] last) {
		this.first = first;
		this.last = last;
	}

	public byte[] getFirst() {
		return first;
	}

	public byte[] getLast() {
		return last;
	}

}
