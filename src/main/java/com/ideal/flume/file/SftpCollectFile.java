package com.ideal.flume.file;

import java.util.Date;

import com.jcraft.jsch.ChannelSftp.LsEntry;

public class SftpCollectFile extends CollectFile {
	public SftpCollectFile(LsEntry file, String dir) {
		this.name = file.getFilename();
		this.size = file.getAttrs().getSize();
		this.timeInMillis = file.getAttrs().getMTime() * 1000L;
		this.time = new Date(timeInMillis);
		this.isDirectory = file.getAttrs().isDir();
		this.absolutePath = dir + name;
	}
}
