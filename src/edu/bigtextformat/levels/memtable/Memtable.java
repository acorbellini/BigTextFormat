package edu.bigtextformat.levels.memtable;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import edu.bigtextformat.block.BlockFormat;
import edu.bigtextformat.levels.LevelOptions;
import edu.bigtextformat.util.LogFile;
import edu.bigtextformat.util.Pair;

public class Memtable {

	public static Memtable fromFile(File path2, LevelOptions opts)
			throws Exception {
		Memtable mem = new Memtable(path2.getPath(), opts);
		mem.log = new LogFile(path2);
		for (byte[] array : mem.log) {
			Operation op = new Operation().fromByteArray(array);
			if (op.op.equals(OperationType.PUT))
				mem.data.put(op.k, op.v);
		}
		return mem;
	}

	public static void updateLogCount(Integer currentCount) {
		if (currentCount > logCount.get())
			logCount.set(currentCount);

	}

	TreeMap<byte[], byte[]> data;

	int estimatedSize = 0;

	private LogFile log;

	private static AtomicInteger logCount = new AtomicInteger(0);

	private String path;

	private LevelOptions opts;

	public Memtable(String path, final LevelOptions opts) {
		this.path = path;
		this.opts = opts;
		data = new TreeMap<byte[], byte[]>(new Comparator<byte[]>() {

			@Override
			public int compare(byte[] arg0, byte[] arg1) {
				return opts.format.compare(arg0, arg1);
			}
		});
	}

	public void clear() throws Exception {

		initLog();
		data.clear();
	}

	public void closeLog() throws IOException {
		if (log != null)
			log.close();
	}

	public boolean contains(byte[] k) {
		return data.containsKey(k);
	}

	public byte[] firstKey() {
		return data.firstKey();
	}

	public byte[] get(byte[] k) {
		return data.get(k);
	}

	public TreeMap<byte[], byte[]> getData() {
		return data;
	}

	public Pair<byte[], byte[]> getFirstIntersect(byte[] from,
			boolean inclFrom, byte[] to, boolean inclTo, BlockFormat format) {
		SortedMap<byte[], byte[]> sm = data.subMap(from, to);
		if (sm.isEmpty())
			return null;
		return Pair.create(sm.firstKey(), sm.get(sm.firstKey()));
	}

	public LogFile getLog() {
		return log;
	}

	private void initLog() throws Exception {
		log = new LogFile(new File(path + "/" + logCount.getAndIncrement()
				+ ".LOG"));
		log.appendMode();
	}

	public boolean isEmpty() throws Exception {
		return estimatedSize == 0;
	}

	public byte[] lastKey() {
		return data.lastKey();
	}

	public long logSize() throws Exception {
		return estimatedSize;
	}

	public String print() {
		StringBuilder builder = new StringBuilder();
		Set<byte[]> es = data.keySet();
		for (byte[] k : es) {
			builder.append(opts.format.print(k));
		}
		return builder.toString();
	}

	public void put(byte[] k, byte[] val) throws Exception {

		Operation e = new Operation(OperationType.PUT, k, val);
		byte[] opAsBytes = e.toByteArray();

		synchronized (this) {
			estimatedSize += opAsBytes.length + 30; // estimated ovhead
			if (log == null)
				initLog();
			log.append(opAsBytes);

			data.put(k, val);
		}
	}
}
