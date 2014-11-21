package edu.bigtextformat.levels;

public enum Operations {
	PUT((byte) 0), DEL((byte) 1);
	private byte id;

	private Operations(byte id) {
		this.id = id;
	}

	public byte getId() {
		return id;
	}
	
	public Operations fromID(byte id) {
		for (Operations op : values()) {
			if(op.getId()==id)
				return op;
		}
		return null;
	}
}
