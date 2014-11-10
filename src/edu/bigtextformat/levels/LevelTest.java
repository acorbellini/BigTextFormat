package edu.bigtextformat.levels;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;

import edu.bigtextformat.record.FormatType;
import edu.bigtextformat.record.FormatTypes;
import edu.bigtextformat.record.RecordFormat;
import edu.jlime.util.DataTypeUtils;
import gnu.trove.TCollections;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.list.array.TIntArrayList;

public class LevelTest {

	public static void main(String[] args) throws Exception {

		// List<Integer> toAdd = Arrays.asList(new Integer[] { 5, 91, 13, 72,
		// 53,
		// 33, 29, 94, 47, 80, 74, 44, 37, 68, 88, 62, 82, 39, 22, 48, 70,
		// 19, 34, 93, 99, 90, 11, 4, 54, 55, 98, 0, 86, 31, 79, 46, 76,
		// 87, 67, 3, 57, 9, 26, 89, 21, 14, 23, 20, 83, 51, 24, 10, 97,
		// 42, 50, 52, 25, 16, 73, 49, 30, 38, 92, 58, 71, 61, 36, 66, 84,
		// 8, 27, 43, 41, 85, 96, 60, 65, 63, 45, 77, 35, 6, 78, 59, 64,
		// 7, 2, 56, 18, 40, 32, 75, 17, 1, 15, 81, 12, 95, 69, 28 });

		// List<Integer> toAdd = Arrays.asList(new Integer[] { 0, 1, 3, 4, 10,
		// 12,
		// 13, 14, 2, 5, 6, 11, 8, 9, 20, 22, 50, 67, 12 });

		// System.out.println(file.print());
		// System.out.println(toAdd);
		// Collections.sort(toAdd);
		// System.out.println(toAdd);
		String PATH = args[0];
		Integer SIZE = Integer.valueOf(args[1]);

		TIntArrayList toAdd = new TIntArrayList(SIZE);
		for (int i = 0; i < SIZE; i++)
			toAdd.add(i);

		// while (true) {
		// RecordFormat format = RecordFormat.create(new String[] { "k",
		// "k2" },
		// new FormatType<?>[] { FormatTypes.INTEGER.getType(),
		// FormatTypes.INTEGER.getType() }, new String[] { "k",
		// "k2" });

		delete(new File(PATH));

		RecordFormat format = RecordFormat.create(new String[] { "k" },
				new FormatType<?>[] { FormatTypes.INTEGER.getType() },
				new String[] { "k" });
		SortedLevelFile file = SortedLevelFile.open(
				PATH,
				new LevelOptions().setFormat(format).setMaxMemTablesWriting(2)
						.setMemTableSize(2 * 1024 * 1024)
						.setBaseSize(2 * 1024 * 1024).setMaxLevel0Files(10)
						.setCompactLevel0Threshold(5).setMaxLevelFiles(10)
						.setMaxBlockSize(256 * 1024));

		toAdd.shuffle(new Random(System.currentTimeMillis()));

		long init = System.currentTimeMillis();
		TIntIterator it = toAdd.iterator();
		int rawSize = 0;
		int cont = 0;
		while (it.hasNext()) {
			if (cont++ % 10000 == 0)
				System.out.println("Inserted " + cont);
			byte[] byteArray = format.newRecord().set("k", it.next())
			// .set("k2", -i)
					.toByteArray();
			byte[] bytes = "Hola!".getBytes();
			file.put(byteArray, bytes);

			rawSize += byteArray.length + bytes.length;
		}
		System.out.println("Raw Size " + rawSize);

		System.out.println(System.currentTimeMillis() - init);

		System.out.println("Compacting...");

		file.compact();

		// System.out.println(file.print());

		System.out.println("Querying...");

		init = System.currentTimeMillis();

		toAdd.sort();

		it = toAdd.iterator();
		while (it.hasNext()) {
			int integer = it.next();
			if (!file.contains(DataTypeUtils.intToByteArray(integer))) {
				throw new Exception("Not inserted! " + integer);
			}
		}
		System.out.println(System.currentTimeMillis() - init);

		System.out.println("Closing...");
		file.close();
		// }

	}

	static void delete(File f) throws IOException {
		if (f.isDirectory()) {
			for (File c : f.listFiles())
				delete(c);
		}
		if (!f.isDirectory() && !f.delete())
			throw new FileNotFoundException("Failed to delete file: " + f);
	}
}
