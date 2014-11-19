package edu.bigtextformat.levels;

public class Pair<T1, T2> {

	private byte[] a;
	private byte[] b;

	public Pair(byte[] a, byte[] b) {
		this.setA(a);
		this.setB(b);
	}

	public byte[] getKey() {
		return a;
	}

	public void setA(byte[] a) {
		this.a = a;
	}

	public byte[] getValue() {
		return b;
	}

	public void setB(byte[] b) {
		this.b = b;
	}

	public static Pair<byte[], byte[]> create(byte[] cs, byte[] cs2) {
		return new Pair<>(cs, cs2);
	}

}
