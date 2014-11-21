package edu.bigtextformat.levels;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Levels implements Iterable<Level> {
	Map<Integer, Level> levels = new ConcurrentHashMap<>();

	@Override
	public Iterator<Level> iterator() {
		return levels.values().iterator();
	}

	public void clear() {
		levels.clear();
	}

	public Level get(int level) {
		return levels.get(level);
	}

	public void put(int level, Level list) {
		levels.put(level, list);
	}
}
