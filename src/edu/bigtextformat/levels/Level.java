package edu.bigtextformat.levels;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import edu.bigtextformat.levels.levelfile.LevelFile;

public class Level implements Iterable<LevelFile> {
	List<LevelFile> files = new ArrayList<>();
	private SortedLevelFile file;
	private int level;

	//
	// private byte[] minKey;
	// private byte[] maxKey;

	public Level(SortedLevelFile sortedLevelFile, int level) {
		this.file = sortedLevelFile;
		this.level = level;
	}

	public synchronized int size() {
		return files.size();
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

	public synchronized void add(LevelFile fl) throws Exception {
		// if (level == 0)
		files.add(fl);
		// else {
		// int pos = search(fl.getMinKey(), files, getOpts().format);
		// if (pos < 0)
		// pos = -(pos + 1);
		// files.add(pos, fl);
		//
		// updateMinAndMax();
		// }
		if (level == 0 && (size() >= getOpts().compactLevel0Threshold))
			file.getCompactor().compact(level);
		// file.getCompactor().setChanged();
		else if (size() > getOpts().maxLevelFiles)
			file.getCompactor().compact(level);
		// file.getCompactor().setChanged();
	}

	private void updateMinAndMax() throws Exception {
		// if (files.isEmpty()) {
		// minKey = null;
		// maxKey = null;
		// } else {
		// minKey = files.get(0).getMinKey();
		// maxKey = files.get(files.size() - 1).getMaxKey();
		// }
	}

	@Override
	public Iterator<LevelFile> iterator() {
		return files.iterator();
	}

	public synchronized void remove(LevelFile from) throws Exception {
		files.remove(from);
		updateMinAndMax();
		notifyAll();
	}

	public synchronized LevelFile get(int i) {
		return files.get(i);
	}

	public int level() {
		return level;
	}

	public LevelOptions getOpts() {
		return file.getOpts();
	}

	public synchronized Set<LevelFile> intersect(byte[] minKey, byte[] maxKey)
			throws Exception {
		Set<LevelFile> found = new HashSet<>();
		for (LevelFile levelFile : files) {
			if (getOpts().format.compare(levelFile.getMinKey(), maxKey) > 0
					|| getOpts().format.compare(levelFile.getMaxKey(), minKey) < 0)
				;
			else
				found.add(levelFile);
		}
		return found;
	}

	public File getCwd() {
		return file.getCwd();
	}

	public int getLastLevelIndex() {
		return file.getLastLevelIndex(level);
	}

	public synchronized void delete(LevelFile file) throws Exception {
		file.delete();
		remove(file);
	}

	public void moveTo(LevelFile from, Level to) throws Exception {
		remove(from);
		from.moveTo(to.level(), file.getLastLevelIndex(to.level()));
		to.add(from);

	}

	public boolean contains(byte[] k) throws Exception {
		List<LevelFile> snapshot = null;
		synchronized (this) {
			if (files.isEmpty())
				return false;
			// if (level > 0
			// && (getOpts().format.compare(k, minKey) < 0 || getOpts().format
			// .compare(k, maxKey) > 0))
			// return false;
			snapshot = new ArrayList<LevelFile>(files);
		}
		for (LevelFile levelFile : snapshot) {
			try {
				if (levelFile.contains(k, getOpts().format))
					return true;
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return false;
	}

	public byte[] get(byte[] k) {
		List<LevelFile> snapshot = null;
		synchronized (this) {
			if (files.isEmpty())
				return null;
			// if (level > 0
			// && (getOpts().format.compare(k, minKey) < 0 || getOpts().format
			// .compare(k, maxKey) > 0))
			// return null;
			snapshot = new ArrayList<LevelFile>(files);
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
}
