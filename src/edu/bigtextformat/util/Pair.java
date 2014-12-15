package edu.bigtextformat.util;

public class Pair<T1, T2> {

	public static Pair<byte[], byte[]> create(byte[] cs, byte[] cs2) {
		return new Pair<>(cs, cs2);
	}
	private byte[] a;

	private byte[] b;

	public Pair(byte[] a, byte[] b) {
		this.setA(a);
		this.setB(b);
	}

	public byte[] getKey() {
		return a;
	}

	public byte[] getValue() {
		return b;
	}

	public void setA(byte[] a) {
		this.a = a;
	}

	public void setB(byte[] b) {
		this.b = b;
	}

}
