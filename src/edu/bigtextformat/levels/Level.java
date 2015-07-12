package edu.bigtextformat.levels;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import edu.bigtextformat.block.BlockFormat;
import edu.bigtextformat.levels.levelfile.LevelFile;
import edu.bigtextformat.util.Pair;

public class Level implements Iterable<LevelFile> {

	public static int search(byte[] k, List<LevelFile> keys, BlockFormat format)
			throws Exception {
		int cont = 0;
		boolean found = false;
		int lo = 0;
		int hi = keys.size() - 1;
		while (lo <= hi && !found) {
			// Key is in a[lo..hi] or not present.
			int mid = lo + (hi - lo) / 2;
			int comp = format.compare(k, keys.get(mid).getMinKey());
			if (comp < 0) {
				hi = mid - 1;
				cont = lo;
			} else if (comp > 0) {
				lo = mid + 1;
				cont = lo;
			} else {
				found = true;
				cont = mid;
			}
			// get(mid);
		}
		if (!found)
			cont = -cont - 1;
		return cont;
	}

	List<LevelFile> files = new ArrayList<>();
	private SortedLevelFile file;

	private volatile int level;
	private ReadWriteLock lock = new ReentrantReadWriteLock();

	private AtomicInteger count = new AtomicInteger(0);
	//
	private volatile byte[] minKey;

	private volatile byte[] maxKey;

	public Level(SortedLevelFile sortedLevelFile, int level) {
		this.file = sortedLevelFile;
		this.level = level;
	}

	public void add(LevelFile fl) throws Exception {

		file.getManifest().put(fl.getLevel(), fl.getCont(), fl.getName(),
				fl.getMinKey(), fl.getMaxKey());

		lock.writeLock().lock();
		try {
			if (fl.getCont() >= count.get())
				count.set(fl.getCont() + 1);

			// if (level == 0)
			int pos = search(fl.getMinKey(), files, file.getOpts().format);
			if (pos < 0)
				pos = -(pos + 1);
			files.add(pos, fl);

			updateMinAndMax();
			// }
			if (level == 0
					&& (files.size() >= getOpts().compactLevel0Threshold))
				file.getCompactor().compact(level);
			// file.getCompactor().setChanged();
			else if (level > 0
					&& files.size() > level * getOpts().maxLevelFiles)
				file.getCompactor().compact(level);
			// file.getCompactor().setChanged();
		} finally {
			lock.writeLock().unlock();
		}
	}

	// public static int search(byte[] minKey, List<LevelFile> keys,
	// BlockFormat format) throws Exception {
	// int cont = 0;
	// boolean found = false;
	// int lo = 0;
	// int hi = keys.size() - 1;
	// while (lo <= hi && !found) {
	// // Key is in a[lo..hi] or not present.
	// int mid = lo + (hi - lo) / 2;
	// int comp = format.compare(minKey, keys.get(mid).getMinKey());
	// if (comp < 0) {
	// hi = mid - 1;
	// cont = lo;
	// } else if (comp > 0) {
	// lo = mid + 1;
	// cont = lo;
	// } else {
	// found = true;
	// cont = mid;
	// }
	// // get(mid);
	// }
	// if (!found)
	// cont = -cont - 1;
	// return cont;
	// }

	public void close() throws Exception {
		for (LevelFile levelFile : files) {
			levelFile.close();
		}
	}

	public boolean contains(byte[] k) throws Exception {
		return get0(k, false) != null;
	}

	public void delete(LevelFile file) throws Exception {
		// lock.writeLock().lock();
		// try {
		remove(file);
		try {
			file.delete();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// } finally {
		// lock.writeLock().unlock();
		// }

	}

	public List<LevelFile> files() {
		lock.readLock().lock();
		try {
			return new ArrayList<>(files);
		} finally {
			lock.readLock().unlock();
		}

	}

	public byte[] get(byte[] k) throws Exception {
		return get0(k, true);
	}

	private byte[] get0(byte[] k, boolean getValue) throws Exception {
		lock.readLock().lock();
		try {
			if (files.isEmpty())
				return null;

			if (level > 0
					&& (getOpts().format.compare(k, minKey) < 0 || getOpts().format
							.compare(k, maxKey) > 0))
				return null;

			if (level > 0) {
				int pos = search(k, files, file.getOpts().format);

				if (pos < 0) {
					pos = -(pos + 1);
					// adjustment to find previous block.
					if (pos > 0)
						pos = pos - 1;
				}

				if (pos < 0 || pos >= files.size())
					return null;
				else {
					if (getValue)
						return files.get(pos).get(k, getOpts().format);
					else {
						if (files.get(pos).contains(k, getOpts().format))
							return new byte[] {};
						else
							return null;
					}
				}
			} else {
				for (LevelFile levelFile : files) {
					try {
						if (getValue) {
							byte[] get = levelFile.get(k, getOpts().format);
							if (get != null)
								return get;
						} else if (levelFile.contains(k, getOpts().format))
							return new byte[] {};

					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
			return null;
		} finally {
			lock.readLock().unlock();
		}
	}

	public File getCwd() {
		return file.getCwd();
	}

	public SortedLevelFile getFile() {
		return file;
	}

	public Pair<byte[], byte[]> getFirstBetween(byte[] from, boolean inclFrom,
			byte[] to, boolean inclTo, BlockFormat format) throws Exception {
		lock.readLock().lock();
		Pair<byte[], byte[]> min = null;
		try {
			if (files.isEmpty())
				return null;

			int toCompare = getOpts().format.compare(to, minKey);
			if (toCompare < 0 || (toCompare == 0 && !inclTo))
				return null;

			int fromCompare = getOpts().format.compare(from, maxKey);
			if (fromCompare > 0 || (toCompare == 0 && !inclFrom))
				return null;

			for (LevelFile levelFile : files) {

				int compare = getOpts().format.compare(to,
						levelFile.getMinKey());
				if (compare < 0 || (!inclTo && compare == 0))
					return null;

				Pair<byte[], byte[]> first = levelFile.getFirstBetween(from,
						inclFrom, to, inclTo);
				if (first != null) {
					if (level == 0) {
						if (min == null
								|| getOpts().format.compare(min.getKey(),
										first.getKey()) > 0)
							min = first;
					} else
						return first;
				}
			}
			return min;
		} finally {
			lock.readLock().unlock();
		}
	}

	public int getLastLevelIndex() {
		return count.getAndIncrement();
	}

	public LevelOptions getOpts() {
		return file.getOpts();
	}

	public LevelFile getRandom() {
		lock.readLock().lock();
		try {
			return files.get((int) (Math.random() * files.size()));
		} finally {
			lock.readLock().unlock();
		}
	}

	public Set<LevelFile> intersect(byte[] min, byte[] max) throws Exception {
		return intersect(min, max, Integer.MAX_VALUE);
	}

	public Set<LevelFile> intersect(byte[] min, byte[] max, int maxSize)
			throws Exception {
		lock.readLock().lock();
		try {
			Set<LevelFile> found = new HashSet<>();

			if (files.isEmpty())
				return found;

			if (getOpts().format.compare(max, minKey) < 0
					|| getOpts().format.compare(min, maxKey) > 0)
				return found;

			for (LevelFile levelFile : files) {
				if (found.size() >= maxSize
						|| getOpts().format.compare(max, levelFile.getMinKey()) < 0)
					return found;
				else if (getOpts().format.compare(levelFile.getMinKey(), max) <= 0
						&& getOpts().format.compare(levelFile.getMaxKey(), min) >= 0)
					found.add(levelFile);
			}
			return found;
		} finally {
			lock.readLock().unlock();
		}
	}

	public int intersectSize(byte[] min, byte[] max) throws Exception {
		lock.readLock().lock();
		try {
			if (files.isEmpty())
				return 0;

			if (getOpts().format.compare(max, minKey) < 0
					|| getOpts().format.compare(min, maxKey) > 0)
				return 0;

			int cont = 0;
			for (LevelFile levelFile : files) {
				if (getOpts().format.compare(max, levelFile.getMinKey()) < 0)
					return cont;
				else if (levelFile.intersectsWith(min, max))
					cont++;
			}
			return cont;
		} finally {
			lock.readLock().unlock();
		}
	}

	@Override
	public Iterator<LevelFile> iterator() {
		return files().iterator();
	}

	public int level() {
		return level;
	}

	public void moveTo(LevelFile from, Level to) throws Exception {
		remove(from);
		from.moveTo(to.level(), file.getLastLevelIndex(to.level()));
		to.add(from);

	}

	public void remove(LevelFile from) throws Exception {
		lock.writeLock().lock();
		try {
			files.remove(from);
			updateMinAndMax();
			synchronized (this) {
				notifyAll();
			}
		} finally {
			lock.writeLock().unlock();
		}
	}

	public int size() {
		lock.readLock().lock();
		try {
			return files.size();
		} finally {
			lock.readLock().unlock();
		}
	}

	private void updateMinAndMax() throws Exception {
		if (files.isEmpty()) {
			minKey = null;
			maxKey = null;
		} else {
			minKey = files.get(0).getMinKey();
			maxKey = files.get(files.size() - 1).getMaxKey();
		}
	}
}
