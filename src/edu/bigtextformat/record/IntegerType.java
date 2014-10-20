package edu.bigtextformat.record;

public class IntegerType implements FormatType<Integer> {

	@Override
	public int compare(byte[] k1, byte[] k2) {
		return 0;
	}

	@Override
	public int size(int offset, byte[] d) {
		return 4;
	}

	@Override
	public byte[] getData(int offset, byte[] d) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Integer get(byte[] k) {
		// TODO Auto-generated method stub
		return null;
	}

}
