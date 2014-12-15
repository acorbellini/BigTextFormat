package edu.bigtextformat.levels.index;

import java.util.Iterator;

public class IndexRangeIterator implements Iterator<Long> {

	private int from;
	private int to;
	private Index i;
	private Long curr = null;

	public IndexRangeIterator(int fromPos, int toPos, Index index) {
		this.from = fromPos;
		this.to = toPos;
		this.i = index;
		if (from <= to && from < i.blocks.size())
			curr = i.blocks.get(from);
	}

	@Override
	public boolean hasNext() {
		return curr != null;
	}

	@Override
	public Long next() {
		Long ret = curr;
		from++;
		if (from <= to && from < i.blocks.size())
			curr = i.blocks.get(from);
		else
			curr = null;
		return ret;
	}

}
