package com.ideal.flume.file;

import java.util.Date;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

public class HdfsCollectFile extends CollectFile {
	public HdfsCollectFile(Path path, FileStatus fileStatus) {
		this.name = path.getName();
		this.size = fileStatus.getLen();
		this.timeInMillis = fileStatus.getModificationTime();
		this.time = new Date(timeInMillis);
		this.isDirectory = fileStatus.isDirectory();
		this.absolutePath = path.toString();
	}
}
