package com.ideal.flume.file;

import org.apache.commons.net.ftp.FTPFile;

public class FtpCollectFile extends CollectFile {
	public FtpCollectFile(FTPFile file, String dir) {
		this.name = file.getName();
		this.size = file.getSize();
		this.timeInMillis = file.getTimestamp().getTimeInMillis();
		this.time = file.getTimestamp().getTime();
		this.isDirectory = file.isDirectory();
		this.absolutePath = dir + name;
	}
}
