package edu.bigtextformat.util;

import java.util.List;

import edu.bigtextformat.block.BlockFormat;

public class Search {
	public static int search(byte[] k, List<byte[]> keys, BlockFormat format) {
		int cont = 0;
		boolean found = false;
		int lo = 0;
		int hi = keys.size() - 1;
		while (lo <= hi && !found) {
			// Key is in a[lo..hi] or not present.
			int mid = lo + (hi - lo) / 2;
			int comp = format.compare(k, keys.get(mid));
			if (comp < 0) {
				hi = mid - 1;
				cont = lo;
			} else if (comp > 0) {
				lo = mid + 1;
				cont = lo;
			} else {
				found = true;
				cont = mid;
			}
			// get(mid);
		}
		if (!found)
			cont = -cont - 1;
		return cont;
	}
}
