package edu.bigtextformat.levels.memtable;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.Set;
import java.util.SortedMap;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import edu.bigtextformat.block.BlockFormat;
import edu.bigtextformat.levels.LevelOptions;
import edu.bigtextformat.raw.RawFile;
import edu.bigtextformat.util.LogFile;
import edu.bigtextformat.util.Pair;
import edu.jlime.util.ByteBuffer;
import edu.jlime.util.DataTypeUtils;

public class Memtable {

	private static final int FLUSH_SIZE = 500000;

	private static final int TIMETOFLUSH = 5000;

	public static Memtable fromFile(File path2, LevelOptions opts, Timer timer)
			throws Exception {
		Memtable mem = new Memtable(path2.getPath(), opts, timer);
		mem.log = new LogFile(path2);
		mem.log.readMode();
		RawFile f = mem.log.getFile();
		int cont = 8;
		long length = f.length();
		boolean finished = false;
		while (cont < length && !finished) {
			try {
				byte id = f.readBytes(cont++, 1)[0];
				OperationType op = OperationType.fromID(id);
				if (op.equals(OperationType.PUT)) {
					int ks = DataTypeUtils.byteArrayToInt(f.readBytes(cont, 4));
					cont += 4;
					byte[] k = f.readBytes(cont, ks);
					cont += k.length;
					int vs = DataTypeUtils.byteArrayToInt(f.readBytes(cont, 4));
					cont += 4;
					byte[] v = f.readBytes(cont, vs);
					cont += v.length;
					mem.data.put(k, v);
				}
			} catch (Exception e) {
				e.printStackTrace();
				finished = true;
			}
		}
		return mem;
	}

	public static void updateLogCount(Integer currentCount) {
		if (currentCount > logCount.get())
			logCount.set(currentCount);

	}

	TreeMap<byte[], byte[]> data;

	int putCont = 0;

	int estimatedSize = 0;

	private volatile LogFile log;

	private static AtomicInteger logCount = new AtomicInteger(0);

	private String path;

	private LevelOptions opts;

	private volatile long lastFlush = 0;

	private TimerTask task;

	public Memtable(String path, final LevelOptions opts, Timer timer) {
		this.path = path;
		this.opts = opts;
		data = new TreeMap<byte[], byte[]>(new Comparator<byte[]>() {

			@Override
			public int compare(byte[] arg0, byte[] arg1) {
				return opts.format.compare(arg0, arg1);
			}
		});

		this.task = new TimerTask() {
			@Override
			public void run() {
				try {
					if (log != null
							&& System.currentTimeMillis() - lastFlush > TIMETOFLUSH) {
						lastFlush = System.currentTimeMillis();
						log.flush();
					}

				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		};
		timer.schedule(task, TIMETOFLUSH, TIMETOFLUSH);
	}

	public void clear() throws Exception {
		initLog();
		data.clear();
	}

	public void closeLog() throws IOException {
		task.cancel();
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
		SortedMap<byte[], byte[]> sm = data.subMap(from, inclFrom, to, inclTo);
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
		log.appendMode(opts.syncmem);
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
		int logSize = 1 + 4 + k.length + 4 + val.length;
		ByteBuffer buff = new ByteBuffer(logSize);
		buff.put(OperationType.PUT.getId());
		buff.putByteArray(k);
		buff.putByteArray(val);
		synchronized (this) {
			estimatedSize += logSize; // estimated
										// ovhead
			if (log == null)
				initLog();
			if (putCont % FLUSH_SIZE == 0
					&& System.currentTimeMillis() - lastFlush > TIMETOFLUSH) {
				lastFlush = System.currentTimeMillis();
				log.flush();
			}
			putCont++;
			log.append(buff.build());
			// log.flush();
			data.put(k, val);
		}
	}
}
