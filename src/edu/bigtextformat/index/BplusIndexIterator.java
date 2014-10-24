package edu.bigtextformat.index;

import java.util.Iterator;

public class BplusIndexIterator implements Iterator<IndexData> {

	private BplusIndex i;

	private IndexData data = null;

	public BplusIndexIterator(BplusIndex bplusIndex) {
		this.i = bplusIndex;
	}

	@Override
	public boolean hasNext() {

		try {
			if (data == null)
				data = i.getFirstData();
			else if (data.getNext() != -1)
				data = IndexData
						.read(i, i.file.getBlock(data.getNext(), false));
			return true;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}

	@Override
	public IndexData next() {
		return data;
	}

}
