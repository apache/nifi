package org.apache.nifi.processors.network.parser.util;

public final class TemplateJSON {
	private final String name;
	private final String type;
	private final int offset;
	private final int length;
	private final String desc;
	private final int id;

	public TemplateJSON(final String name, final String type, final int offset, final int length, final String desc,
			final int id) {
		this.name = name;
		this.type = type;
		this.offset = offset;
		this.length = length;
		this.desc = desc;
		this.id = id;
	}

	public int getOffset() {
		return offset;
	}

	public int getLength() {
		return length;
	}

	public String getDesc() {
		return desc;
	}

	public int getId() {
		return id;
	}

	public String getName() {
		return name;
	}

	public String getType() {
		return type;
	}
}
