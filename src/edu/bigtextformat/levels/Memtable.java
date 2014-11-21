package edu.bigtextformat.levels;

import java.io.IOException;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import edu.bigtextformat.block.BlockFile;
import edu.bigtextformat.block.BlockFormat;
import edu.jlime.util.DataTypeUtils;

public class Memtable {

	// List<Operation> table = new ArrayList<>();

	TreeMap<byte[], byte[]> data;

	private int size = 0;

	private BlockFile log;

	int logCount = 0;

	private String path;

	private BlockFormat format;

	public Memtable(String path, BlockFormat format) throws Exception {
		this.path = path;
		initLog();
		this.format = format;
		data = new TreeMap<byte[], byte[]>(format);
	}

	private void initLog() throws Exception {
		log = BlockFile.appendOnly(path + "/LOG." + logCount++,
				DataTypeUtils.byteArrayToLong("MANIFEST".getBytes()));
	}

	public synchronized void put(byte[] k, byte[] val) throws Exception {
		Operation e = new Operation(Operations.PUT.getId(), k, val);
		// table.add(e);
		byte[] opAsBytes = e.toByteArray();
		log.newFixedBlock(opAsBytes);
		size += opAsBytes.length;
	}

	public int size() {
		return size;
	}

	public byte[] lastKey() {
		// return null;
		return data.lastKey();
	}

	public byte[] firstKey() {
		// return null;
		return data.firstKey();
	}

	public boolean contains(byte[] k) {
		// return false;
		return data.containsKey(k);
	}

	public String print() {
		StringBuilder builder = new StringBuilder();
		Set<byte[]> es = data.keySet();
		for (byte[] k : es) {
			builder.append(format.print(k));
		}
		return builder.toString();
	}

	public void closeLog() throws IOException {
		log.close();
	}

	public String getLogName() {
		return log.getRawFile().getFile().getName();
	}

	public void clear() throws Exception {

		initLog();
		data.clear();
		size = 0;
	}

	public Pair<byte[], byte[]> getFirstIntersect(byte[] from,
			boolean inclFrom, byte[] to, boolean inclTo, BlockFormat format) {
		// return null;
		// int cont = Search.search(from, keys, format);
		// if (cont < 0)
		// cont = -(cont + 1);
		// else if (!inclFrom)
		// cont = cont + 1;
		//
		// if (cont >= keys.size())
		// return null;
		//
		// byte[] cs = keys.get(cont);
		// int compare = format.compare(cs, to);
		// if (compare > 0)
		// return null;
		//
		// if (compare == 0 && !inclTo)
		// return null;
		//
		// return Pair.create(cs, values.get(cont));

		SortedMap<byte[], byte[]> sm = data.subMap(from, to);
		if (sm.isEmpty())
			return null;
		return Pair.create(sm.firstKey(), sm.get(sm.firstKey()));
	}

	public byte[] get(byte[] k) {
		return data.get(k);
	}
}
