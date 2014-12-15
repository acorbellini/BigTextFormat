package edu.bigtextformat.levels;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import edu.bigtextformat.levels.levelfile.LevelFile;

public class Levels implements Iterable<Level> {
	Map<Integer, Level> levels = new ConcurrentHashMap<>();

	public void clear() {
		levels.clear();
	}

	public Level get(int level) {
		return levels.get(level);
	}

	public HashMap<String, LevelFile> getMap() {
		HashMap<String, LevelFile> ret = new HashMap<>();
		for (Level l : levels.values()) {
			for (LevelFile levelFile : l) {
				ret.put(levelFile.getName(), levelFile);
			}
		}
		return ret;
	}

	@Override
	public Iterator<Level> iterator() {
		return levels.values().iterator();
	}

	public void put(int level, Level list) {
		levels.put(level, list);
	}
}
