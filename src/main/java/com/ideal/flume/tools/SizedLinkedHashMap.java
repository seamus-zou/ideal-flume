package com.ideal.flume.tools;

import java.util.LinkedHashMap;
import java.util.Map.Entry;

public class SizedLinkedHashMap<K, V> extends LinkedHashMap<K, V> {
	private static final long serialVersionUID = -1837076529287450398L;
	private final int maxOpenFiles;

	public SizedLinkedHashMap(int maxOpenFiles) {
		super(16, 0.75f, true); // stock initial capacity/load, access
		// ordering
		this.maxOpenFiles = maxOpenFiles;
	}

	@Override
	protected boolean removeEldestEntry(Entry<K, V> eldest) {
		return size() > maxOpenFiles;
	}
}
