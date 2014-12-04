package edu.bigtextformat.block;

public class CorruptedFileException extends Exception {

	public CorruptedFileException(BlockFile blockFile, long pos, Exception cause) {
		super("Corrupted File " + blockFile + " detected while reading " + pos,
				cause);
	}

}
