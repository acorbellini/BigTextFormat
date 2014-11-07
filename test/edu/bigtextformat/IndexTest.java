package edu.bigtextformat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import edu.bigtextformat.index.BplusIndex;
import edu.bigtextformat.index.IndexData;
import edu.bigtextformat.record.FormatType;
import edu.bigtextformat.record.FormatTypes;
import edu.bigtextformat.record.RecordFormat;
import edu.jlime.util.DataTypeUtils;

public class IndexTest {
	public static void main(String[] args) throws Exception {
		final RecordFormat format = RecordFormat.create(new String[] { "k",
				"k2" }, new FormatType<?>[] { FormatTypes.LONG.getType(),
				FormatTypes.LONG.getType() }, new String[] { "k", "k2" });

		long start = System.currentTimeMillis();

		ExecutorService exec = Executors.newFixedThreadPool(100);
		final Semaphore sem = new Semaphore(120);
		final BplusIndex i = new BplusIndex(
				"C:/Users/Ale/Desktop/indextest.bplus", format, true, true);
		List<Long> random = new ArrayList<>();
		for (long j = 0; j < 500; j++) {
			random.add(j);
		}
		// Scanner scan = new Scanner(new File("input.txt"));
		// while (scan.hasNext()) {
		// random.add(scan.nextInt());
		// }
		// scan.close();

		Collections.shuffle(random);

		final AtomicInteger contPut = new AtomicInteger(0);
		for (Long j : random) {
			final long curr = j;
			// put(i, format, "k", Integer.valueOf(curr));
			sem.acquire();

			exec.execute(new Runnable() {
				@Override
				public void run() {

					try {
						// if (curr == 2)
						// System.out.println("What?!?!");
						for (long j = 0; j < 1000; j++) {
							put(i, format, "k", Long.valueOf(curr), "k2",
									Long.valueOf(j));
							int incrementAndGet = contPut.incrementAndGet();
							if (incrementAndGet % 10000 == 0)
								System.out.println(incrementAndGet);
						}

						// try {
						// check2(i);
						// } catch (Exception e) {
						// System.out.println("broke on line " + contPut
						// + " with element " + curr);
						//
						// e.printStackTrace();
						//
						// System.out.println(i.print());
						//
						// return;
						// }

					} catch (Exception e) {
						e.printStackTrace();
					} finally {
						sem.release();
					}
				}
			});
		}

		exec.shutdown();
		exec.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);

		System.out.println((System.currentTimeMillis() - start) / 1000);

		// System.out.println(i.print());
		try {
			// check(i);
		} catch (Exception e) {
			e.printStackTrace();
			return;
		} finally {
			i.close();
		}

	}

	private static void check2(final BplusIndex i) throws Exception {
		int cont = 0;
		int max = -1;
		for (IndexData data : i) {
			for (byte[] b : data.getKeys()) {
				int curr = DataTypeUtils.byteArrayToInt(b);
				if (curr < max) {
					throw new Exception("Not ok on element " + curr);
				} else
					max = curr;
			}
		}
	}

	private static void check(final BplusIndex i) throws Exception {
		int cont = 0;
		for (IndexData data : i) {
			for (byte[] b : data.getKeys()) {
				if (DataTypeUtils.byteArrayToInt(b) != cont++) {
					throw new Exception("Not ok: failed on " + cont);
				}
			}
		}
	}

	private static void put(BplusIndex i, RecordFormat format, String k,
			Long val, String k2, Long val2) throws Exception {
		i.put(format.newRecord().set(k, val).set(k2, val2).toByteArray(), val
				.toString().getBytes(), true);
	}
}
