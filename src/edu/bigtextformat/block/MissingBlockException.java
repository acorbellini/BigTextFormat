package edu.bigtextformat.block;

import edu.bigtextformat.raw.RawFile;

public class MissingBlockException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3298111517211850647L;

	public MissingBlockException(long pos, RawFile file) {
		super("Block on pos " + pos + " does not exist at file "
				+ file.getFile().getPath());
	}

}
