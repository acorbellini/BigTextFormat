package edu.bigtextformat.util;

import gnu.trove.list.array.TByteArrayList;
import gnu.trove.list.array.TIntArrayList;

import java.util.AbstractList;
import java.util.Collections;
import java.util.Comparator;

public class ByteArrayList extends AbstractList<byte[]> {
	public static void main(String[] args) {
		ByteArrayList list = new ByteArrayList();
		String[] toAdd = new String[] { "Hola", "0Aasdsadsad", "Que tal?!?!",
				"2", "3", "4", "05", "7", "10", "58" };
		for (String l : toAdd) {
			list.add(l.getBytes());
			// DataTypeUtils.longToByteArray(l)
		}
		Comparator<byte[]> comparator = new Comparator<byte[]>() {

			@Override
			public int compare(byte[] arg0, byte[] arg1) {
				return new String(arg0).compareTo(new String(arg1));
				// return Long.compare(DataTypeUtils.byteArrayToLong(arg0),
				// DataTypeUtils.byteArrayToLong(arg1));
			}

		};
		Collections.sort(list, comparator);

		int pos = Collections.binarySearch(list, "Que tal?!?!".getBytes(),
				comparator);

		System.out.println(new String(list.get(pos)));

		for (byte[] l : list) {
			System.out.println(new String(l));
			// System.out.println(DataTypeUtils.byteArrayToLong(l));
		}
	}
	TByteArrayList vals;

	TIntArrayList indexes;

	public ByteArrayList() {
		vals = new TByteArrayList();
		indexes = new TIntArrayList();
	}

	public ByteArrayList(byte[] byteArray, int[] intArray) {
		this.vals = new TByteArrayList(byteArray);
		this.indexes = new TIntArrayList(intArray);
	}

	@Override
	public boolean add(byte[] e) {
		indexes.add(vals.size());
		vals.add(e);
		return true;
	}

	@Override
	public void clear() {
		vals.clear();
		indexes.clear();
	}

	@Override
	public byte[] get(int index) {
		int current = indexes.get(index);
		int len = (index + 1 < indexes.size() ? indexes.get(index + 1) : vals
				.size()) - current;
		return vals.toArray(current, len);
	}

	public TIntArrayList getIndexes() {
		return indexes;
	}

	public TByteArrayList getVals() {
		return vals;
	}

	public void insert(int index, byte[] k) {
		if (index > indexes.size())
			throw new Error(index + " > " + indexes.size());
		if (index == indexes.size()) {
			add(k);
			return;
		}
		int current = indexes.get(index);
		indexes.insert(index, current);
		vals.insert(current, k);
		for (int i = index + 1; i < indexes.size(); i++) {
			int curr = indexes.get(i);
			indexes.set(i, curr + k.length);
		}
	}

	@Override
	public byte[] set(int index, byte[] element) {
		if (index > indexes.size())
			throw new Error(index + " > " + indexes.size());
		int current = indexes.get(index);
		int next = (index + 1 < indexes.size() ? indexes.get(index + 1) : vals
				.size());
		byte[] ret = vals.toArray(current, next - current);

		int sizeDiff = element.length - (next - current);
		if (sizeDiff < 0) {
			vals.remove(current, -sizeDiff);
		} else {
			for (int i = 0; i < sizeDiff; i++) {
				vals.insert(current, (byte) 0x0b);
			}
		}

		for (int i = index + 1; i < indexes.size(); i++) {
			int curr = indexes.get(i);
			indexes.set(i, curr + sizeDiff);
		}

		vals.set(current, element);
		return ret;
	}

	@Override
	public int size() {
		return indexes.size();
	}
}
