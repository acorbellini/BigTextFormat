package edu.bigtextformat.levels;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import edu.bigtextformat.block.Block;
import edu.bigtextformat.block.BlockFile;
import edu.bigtextformat.levels.levelfile.LevelFile;
import edu.jlime.util.ByteBuffer;
import edu.jlime.util.DataTypeUtils;

public class Manifest {

	BlockFile log;

	HashMap<String, LevelFile> files = new HashMap<>();

	int maxCont = 0;

	long id = System.nanoTime();

	private String path;

	private File fDir;

	public Manifest(File fDir) throws Exception {
		this.path = fDir.getPath() + "/MANIFEST.log";
		this.fDir = fDir;
		try {
			log = BlockFile.open(path,
					DataTypeUtils.byteArrayToLong("MANIFEST".getBytes()));
		} catch (Exception e) {
		}
		if (log == null || log.isEmpty()) {
			if (log != null && log.isEmpty())
				log.delete();
			log = BlockFile.appendOnly(path,
					DataTypeUtils.byteArrayToLong("MANIFEST".getBytes()));
		} else {
			log = compact(log, true);
		}

	}

	public synchronized BlockFile compact(BlockFile oldLog, boolean addFiles)
			throws Exception, IOException {
		String newPath = fDir.getPath() + "/MANIFEST.log.new";
		BlockFile current = BlockFile.appendOnly(newPath,
				DataTypeUtils.byteArrayToLong("MANIFEST".getBytes()));
		try {
			for (Block block : oldLog) {
				ByteBuffer buff = new ByteBuffer(block.payload());
				int level = buff.getInt();
				int cont = buff.getInt();
				String levelFileName = buff.getString();
				byte[] minKey = buff.getByteArray();
				byte[] maxKey = buff.getByteArray();

				if (Files
						.exists(Paths.get(fDir.getPath() + "/" + levelFileName))) {
					if (cont > maxCont)
						maxCont = cont;
					if (!files.containsKey(levelFileName))
						files.put(
								levelFileName,
								LevelFile.open(level, cont, fDir.getPath()
										+ "/" + levelFileName, minKey, maxKey));
					else {
						System.out.println("Was already there.");
					}
					put(current, level, cont, levelFileName, minKey, maxKey);
				}
			}

			for (File currentFiles : fDir.listFiles()) {
				String name = currentFiles.getName();
				if (name.endsWith(".sst") && !files.containsKey(name)) {
					Integer level = Integer.valueOf(name.substring(0,
							name.indexOf("-")));
					Integer cont = Integer.valueOf(name.substring(
							name.indexOf("-") + 1, name.indexOf(".")));
					LevelFile open = LevelFile.open(level, cont, fDir.getPath()
							+ "/" + name, null, null);
					files.put(currentFiles.getName(), open);
					put(current, level, cont, name, open.getMinKey(),
							open.getMaxKey());
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			oldLog.delete();
			current.close();
		}

		Files.move(Paths.get(newPath), Paths.get(path));
		return BlockFile.appendOnly(path,
				DataTypeUtils.byteArrayToLong("MANIFEST".getBytes()));
	}

	public synchronized void compact() throws Exception {
		log.close();
		log = BlockFile.open(path,
				DataTypeUtils.byteArrayToLong("MANIFEST".getBytes()));
		log = compact(log, false);
	}

	public int getMaxCount() {
		return maxCont;
	}

	public LevelFile getFirstLevelFile() {
		return files.get(0);
	}

	public Collection<LevelFile> readLevelFiles() {
		Collection<LevelFile> ret = files.values();
		files = new HashMap<>();
		return ret;
	}

	public void put(BlockFile current, int level, int cont,
			String levelFileName, byte[] minKey, byte[] maxKey)
			throws Exception {
		ByteBuffer buff = new ByteBuffer();
		buff.putInt(level);
		buff.putInt(cont);
		buff.putString(levelFileName);
		buff.putByteArray(minKey);
		buff.putByteArray(maxKey);
		synchronized (this) {
			current.newBlock(buff.build());
		}
	}

	public void put(int level, int cont, String levelFileName, byte[] minKey,
			byte[] maxKey) throws Exception {
		put(log, level, cont, levelFileName, minKey, maxKey);
	}

	public void close() throws IOException {
		log.close();
	}

}
