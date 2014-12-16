package edu.bigtextformat;

import java.util.Collections;
import java.util.Comparator;

import edu.bigtextformat.util.ByteArrayList;

public class ByteArrayListTest {
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
}
