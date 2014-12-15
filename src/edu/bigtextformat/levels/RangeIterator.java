package edu.bigtextformat.levels;

import java.util.Iterator;

import edu.bigtextformat.util.Pair;

public class RangeIterator implements Iterator<Pair<byte[], byte[]>> {

	private SortedLevelFile file;
	private byte[] to;

	private Pair<byte[], byte[]> curr = null;

	public RangeIterator(SortedLevelFile sortedLevelFile, byte[] from, byte[] to)
			throws Exception {
		this.file = sortedLevelFile;
		this.to = to;
		this.curr = file.getFirstInIntersection(from, true, to, true);
		if (curr!=null && file.compare(curr.getKey(), to) > 0)
			curr = null;
	}

	@Override
	public boolean hasNext() {
		return curr != null && file.compare(to, curr.getKey()) >= 0;
	}

	@Override
	public Pair<byte[], byte[]> next() {
		Pair<byte[], byte[]> ret = curr;
		try {
			curr = file.getFirstInIntersection(curr.getKey(), false, to, true);
		} catch (Exception e) {
			curr = null;
		}
		return ret;
	}

}
