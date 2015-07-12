package edu.bigtextformat.util;

import edu.bigtextformat.levels.LevelOptions;
import edu.bigtextformat.levels.SortedLevelFile;
import edu.bigtextformat.record.FormatType;
import edu.bigtextformat.record.FormatTypes;
import edu.bigtextformat.record.RecordFormat;
import edu.jlime.util.DataTypeUtils;
import edu.jlime.util.compression.CompressionType;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.list.array.TIntArrayList;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Random;

public class LevelTest {

	static void delete(File f) throws IOException {
		if (f.isDirectory()) {
			for (File c : f.listFiles())
				delete(c);
		}
		if (!f.isDirectory() && !f.delete())
			throw new FileNotFoundException("Failed to delete file: " + f);
	}

	public static void main(String[] args) throws Exception {
		String PATH = args[0];
		Integer SIZE = Integer.valueOf(args[1]);

		TIntArrayList toAdd = new TIntArrayList(SIZE);
		for (int i = 0; i < SIZE; i++)
			toAdd.add(i);
		// toAdd.addAll(testSet);

		// while (true) {
		// RecordFormat format = RecordFormat.create(new String[] { "k",
		// "k2" },
		// new FormatType<?>[] { FormatTypes.INTEGER.getType(),
		// FormatTypes.INTEGER.getType() }, new String[] { "k",
		// "k2" });

		// delete(new File(PATH));

		RecordFormat format = RecordFormat.create(new String[] { "k" },
				new FormatType<?>[] { FormatTypes.INTEGER.getType() },
				new String[] { "k" });
		LevelOptions opts = new LevelOptions().setFormat(format)
				.setSegmentWriters(5).setMemTableSize(8 * 1024 * 1024)
				.setBaseSize(2 * 1024 * 1024).setMaxLevel0Files(4)
				.setMaxCompactionWriters(4).setMaxCompactorThreads(4)
				.setCompactLevel0Threshold(4).setMaxLevelFiles(10)
				.setMaxBlockSize(512 * 1024)
				// .setAppendOnly(true)
				.setCompressed(CompressionType.BZIP.getComp());
		SortedLevelFile file = SortedLevelFile.open(PATH, opts);

		file.compact();

		if (JUST_OPEN) {
			file.close();
			return;
		}

		toAdd.shuffle(new Random(System.currentTimeMillis()));
		if (LOAD) {
			long init = System.currentTimeMillis();
			TIntIterator it = toAdd.iterator();
			int rawSize = 0;
			int cont = 0;
			long last = init;
			while (it.hasNext()) {
				if (++cont % 100000 == 0) {
					System.out
							.println("Inserted "
									+ cont
									+ " Rate "
									+ (100000000 / (float) (System
											.currentTimeMillis() - last))
									+ " ins per sec");
					last = System.currentTimeMillis();
					// file.compact();
				}
				byte[] byteArray = format.newRecord().set("k", it.next())
				// .set("k2", -i)
						.toByteArray();
				byte[] bytes = "Hola!".getBytes();
				file.put(byteArray, bytes);

				rawSize += byteArray.length + bytes.length;
			}
			System.out.println("Raw Size " + rawSize);

			System.out.println(System.currentTimeMillis() - init);

			// System.out.println(file.print());

			// System.out.println("Compacting...");
			// long initCompact = System.currentTimeMillis();
			// file.compact();
			// System.out.println("Compact time: "
			// + (System.currentTimeMillis() - initCompact));

			// System.out.println(file.print());

			System.out.println("Closing...");
			file.close();

			System.out.println("Reopening...");
			file = SortedLevelFile.open(PATH, opts);
			// file.compact();
		}
		System.out.println("Querying...");

		toAdd.sort();

		int contQuery = 0;

		long queryInit = System.currentTimeMillis();
		TIntIterator itQuery = toAdd.iterator();
		while (itQuery.hasNext()) {
			int integer = itQuery.next();
			if (contQuery % 100000 == 0)
				System.out.println("Current query count " + contQuery);
			if (!file.contains(DataTypeUtils.intToByteArray(integer))) {
				// System.out.println(toAdd);
				// System.out.println(file.print());
				throw new Exception("Not inserted! " + integer);
			}
			contQuery++;
		}

		System.out.println("Query Time "
				+ (System.currentTimeMillis() - queryInit));
		System.out.println("Closing...");
		file.close();
		// }
	}

	private static final boolean LOAD = true;

	private static final boolean JUST_OPEN = false;
}
