package edu.bigtextformat;

import java.util.ArrayList;

import edu.bigtextformat.data.DataImpl;
import edu.bigtextformat.index.Index;

public class BigText {
	Dictionary dict;
	Data data;
	ArrayList<Index> additional;

	public BigText(String path) throws Exception {
		data = new DataImpl(path);
		dict = new DictionaryImpl(path + ".dict");
		additional = findIndexes(path);
	}

	private ArrayList<Index> findIndexes(String path) {
		return new ArrayList<Index>();
	}
}
