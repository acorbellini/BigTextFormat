package edu.bigtextformat.levels;

import java.io.File;

public class EmptyDBException extends Exception {
	public EmptyDBException(File dir) {
		super("No level files found on dir " + dir);
	}
}
