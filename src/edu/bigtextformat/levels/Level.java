package edu.bigtextformat.levels;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import edu.bigtextformat.levels.levelfile.LevelFile;

public class Level implements Iterable<LevelFile> {
	List<LevelFile> files = new ArrayList<>();
	private SortedLevelFile file;
	private int level;

	private ReadWriteLock lock = new ReentrantReadWriteLock();
	private AtomicInteger count = new AtomicInteger(0);

	//
	private byte[] minKey;
	private byte[] maxKey;

	public SortedLevelFile getFile() {
		return file;
	}

	public Level(SortedLevelFile sortedLevelFile, int level) {
		this.file = sortedLevelFile;
		this.level = level;
	}

	public int size() {
		lock.readLock().lock();
		try {
			return files.size();
		} finally {
			lock.readLock().unlock();
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

	public void add(LevelFile fl) throws Exception {

		file.getManifest().put(fl.getLevel(), fl.getCont(),
				fl.getFile().getRawFile().getFile().getName(), fl.getMinKey(),
				fl.getMaxKey());

		lock.writeLock().lock();
		try {
			if (fl.getCont() >= count.get())
				count.set(fl.getCont() + 1);

			// if (level == 0)
			int pos = Collections.binarySearch(files, fl,
					new Comparator<LevelFile>() {
						@Override
						public int compare(LevelFile o1, LevelFile o2) {
							try {
								return getOpts().format.compare(o1.getMaxKey(),
										o2.getMaxKey());
							} catch (Exception e) {
								e.printStackTrace();
							}
							return 0;
						}

					});
			if (pos < 0)
				pos = -(pos + 1);
			files.add(pos, fl);
			// else {
			// int pos = search(fl.getMinKey(), files, getOpts().format);
			// if (pos < 0)
			// pos = -(pos + 1);
			// files.add(pos, fl);
			//
			updateMinAndMax();
			// }
			if (level == 0 && (size() >= getOpts().compactLevel0Threshold))
				file.getCompactor().compact(level);
			// file.getCompactor().setChanged();
			else if (size() > getOpts().maxLevelFiles)
				file.getCompactor().compact(level);
			// file.getCompactor().setChanged();
		} finally {
			lock.writeLock().unlock();
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

	@Override
	public Iterator<LevelFile> iterator() {
		return files.iterator();
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

	public LevelFile get(int i) {
		lock.readLock().lock();
		try {
			return files.get(i);
		} finally {
			lock.readLock().unlock();
		}

	}

	public int level() {
		return level;
	}

	public LevelOptions getOpts() {
		return file.getOpts();
	}

	public Set<LevelFile> intersect(byte[] minKey, byte[] maxKey)
			throws Exception {
		lock.readLock().lock();
		try {
			Set<LevelFile> found = new HashSet<>();
			for (LevelFile levelFile : files) {
				if (getOpts().format.compare(levelFile.getMinKey(), maxKey) > 0
						|| getOpts().format.compare(levelFile.getMaxKey(),
								minKey) < 0)
					;
				else
					found.add(levelFile);
			}
			return found;
		} finally {
			lock.readLock().unlock();
		}
	}

	public File getCwd() {
		return file.getCwd();
	}

	public int getLastLevelIndex() {
		return count.getAndIncrement();
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

	public void moveTo(LevelFile from, Level to) throws Exception {
		remove(from);
		from.moveTo(to.level(), file.getLastLevelIndex(to.level()));
		to.add(from);

	}

	public boolean contains(byte[] k) throws Exception {
		// List<LevelFile> snapshot = null;
		lock.readLock().lock();

		try {
			if (files.isEmpty())
				return false;
			// snapshot = new ArrayList<LevelFile>(files);
			//
			// if (level > 0)
			// LevelMerger.sortByKey(snapshot, getOpts().format);
			for (LevelFile levelFile : files) {
				try {
					if (levelFile.contains(k, getOpts().format))
						return true;
					else if (level > 0
							&& getOpts().format.compare(k,
									levelFile.getMinKey()) < 0) {
						return false;
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			return false;
		} finally {
			lock.readLock().unlock();
		}
	}

	public byte[] get(byte[] k) {
		List<LevelFile> snapshot = null;
		lock.readLock().lock();
		try {
			if (files.isEmpty())
				return null;
			if (level > 0
					&& (getOpts().format.compare(k, minKey) < 0 || getOpts().format
							.compare(k, maxKey) > 0))
				return null;
			snapshot = new ArrayList<LevelFile>(files);
		} finally {
			lock.readLock().unlock();
		}

		byte[] ret = null;
		for (LevelFile levelFile : snapshot) {
			try {
				ret = levelFile.get(k, getOpts().format);
				if (ret != null)
					return ret;
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return null;
	}

	public void close() throws Exception {
		for (LevelFile levelFile : files) {
			levelFile.close();
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
				if (level > 0
						&& getOpts().format.compare(min, levelFile.getMaxKey()) > 0)
					return cont;
				else if (!(getOpts().format.compare(levelFile.getMinKey(), max) < 0 || getOpts().format
						.compare(min, levelFile.getMaxKey()) > 0))
					cont++;
			}
			return cont;
		} finally {
			lock.readLock().unlock();
		}
	}
}
