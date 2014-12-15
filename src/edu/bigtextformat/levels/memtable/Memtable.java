package edu.bigtextformat.levels.memtable;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import edu.bigtextformat.block.BlockFormat;
import edu.bigtextformat.logfile.LogFile;
import edu.bigtextformat.util.Pair;

public class Memtable {

	private static final int FLUSH_MAP_SIZE = 1000000;

	// List<Operation> table = new ArrayList<>();

	TreeMap<byte[], byte[]> data;

	int estimatedSize = 0;

	private LogFile log;

	private static AtomicInteger logCount = new AtomicInteger(0);

	private String path;

	private BlockFormat format;

	public Memtable(String path, final BlockFormat format) {
		this.path = path;
		// initLog();
		this.format = format;
		data = new TreeMap<byte[], byte[]>(new Comparator<byte[]>() {

			@Override
			public int compare(byte[] arg0, byte[] arg1) {
				return format.compare(arg0, arg1);
			}
		});
	}

	private void initLog() throws Exception {
		log = new LogFile(new File(path + "/" + logCount.getAndIncrement()
				+ ".LOG"));
		log.appendMode();
	}

	public void put(byte[] k, byte[] val) throws Exception {

		Operation e = new Operation(OperationType.PUT, k, val);
		// table.add(e);
		byte[] opAsBytes = e.toByteArray();

		synchronized (this) {
			estimatedSize += opAsBytes.length + 30; // estimated ovhead
			if (log == null)
				initLog();
			log.append(opAsBytes);

			data.put(k, val);
		}

		// if (data.size() % FLUSH_MAP_SIZE == 0) {
		// log.flush();
		// }
	}

	public long logSize() throws Exception {
		// if (log == null)
		// return 0;
		// return log.size();
		return estimatedSize;
	}

	public boolean isEmpty() throws Exception {
		// if (log == null)
		// return true;
		// return log.isEmpty();
		return estimatedSize == 0;
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
		if (log != null)
			log.close();
	}

	public LogFile getLog() {
		return log;
	}

	public void clear() throws Exception {

		initLog();
		data.clear();
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

	public static void updateLogCount(Integer currentCount) {
		if (currentCount > logCount.get())
			logCount.set(currentCount);

	}

	public static Memtable fromFile(File path2, BlockFormat format)
			throws Exception {
		Memtable mem = new Memtable(path2.getPath(), format);
		mem.log = new LogFile(path2);
		for (byte[] array : mem.log) {
			Operation op = new Operation().fromByteArray(array);
			if (op.op.equals(OperationType.PUT))
				mem.data.put(op.k, op.v);
		}
		return mem;
	}
	
	public TreeMap<byte[], byte[]> getData() {
		return data;
	}
}
