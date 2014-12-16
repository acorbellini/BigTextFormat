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
			if (data == null){
				data = i.getFirstData();
				return true;
			}else if (data.getNext() != -1) {
				data =i.getIndexData(data.getNext());
				return true;
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}

	@Override
	public IndexData next() {
		return data;
	}

	@Override
	public void remove() {
		// TODO Auto-generated method stub
		
	}

}
