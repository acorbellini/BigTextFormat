package edu.bigtextformat;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

public class RandomAccessFileTest {
	public static void main(String[] args) throws Exception {
		new RandomAccessFileTest().run();
	}

	private void run() throws Exception {
		final RandomAccessFile f1 = new RandomAccessFile(new File("test.txt"),
				"rw");
		final RandomAccessFile f2 = new RandomAccessFile(new File("test.txt"),
				"rw");
		final RandomAccessFile f3 = new RandomAccessFile(new File("test.txt"),
				"rw");

		final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

		ExecutorService exec = Executors.newFixedThreadPool(3);
		exec.execute(new Runnable() {

			@Override
			public void run() {
				writeAndShowMessage(f1, lock, "Thread 1 ");
			}
		});

		exec.execute(new Runnable() {

			@Override
			public void run() {
				writeAndShowMessage(f2, lock, "Thread 2 ");
			}
		});

		exec.execute(new Runnable() {

			@Override
			public void run() {
				writeAndShowMessage(f3, lock, "Thread 3 ");
			}
		});
		exec.shutdown();
		exec.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
		f1.close();
		f2.close();
		f3.close();
	}

	private void writeAndShowMessage(final RandomAccessFile f1,
			final ReentrantReadWriteLock lock, String msg) {
		long pos = 0;
		int size = 0;
		WriteLock wl = lock.writeLock();
		wl.lock();
		try {
			pos = f1.length();
			byte[] bytes = (msg).getBytes();
			size = bytes.length;
			f1.seek(pos);

			f1.write(bytes);
			f1.getFD().sync();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			wl.unlock();
		}

		ReadLock rl = lock.readLock();
		rl.lock();
		try {
			System.out.println(lock.getReadLockCount());
			f1.seek(pos);
			byte[] chars = new byte[size];
			f1.read(chars);
			System.out.println(new String(chars));
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			rl.unlock();
		}
	}
}
