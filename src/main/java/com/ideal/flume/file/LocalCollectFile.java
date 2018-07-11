package com.ideal.flume.file;

import java.io.File;
import java.util.Date;

public class LocalCollectFile extends CollectFile {
	public LocalCollectFile(File file) {
		this.name = file.getName();
		this.size = file.length();
		this.timeInMillis = file.lastModified();
		this.time = new Date(file.lastModified());
		this.isDirectory = file.isDirectory();
		this.absolutePath = file.getAbsolutePath();
	}
}
