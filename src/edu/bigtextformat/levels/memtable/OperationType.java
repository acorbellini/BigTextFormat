package edu.bigtextformat.levels.memtable;

public enum OperationType {
	PUT((byte) 0), DEL((byte) 1);
	public static OperationType fromID(byte id) {
		for (OperationType op : values()) {
			if (op.getId() == id)
				return op;
		}
		return null;
	}

	private byte id;

	private OperationType(byte id) {
		this.id = id;
	}

	public byte getId() {
		return id;
	}
}
