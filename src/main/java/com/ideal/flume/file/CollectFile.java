package com.ideal.flume.file;

import java.util.Date;

public abstract class CollectFile {
	protected String name;
	protected long size;
	protected long timeInMillis;
	protected Date time;
	protected boolean isDirectory;
	protected String absolutePath;

	public String getName() {
		return name;
	}

	public long getSize() {
		return size;
	}

	public long getTimeInMillis() {
		return timeInMillis;
	}

	public Date getTime() {
		return time;
	}

	public boolean isDirectory() {
		return isDirectory;
	}

	public String getAbsolutePath() {
		return absolutePath;
	}

	public void setAbsolutePath(String absolutePath) {
		this.absolutePath = absolutePath;
	}

}
