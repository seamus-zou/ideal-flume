package com.ideal.flume.serialization;

import org.apache.flume.serialization.LengthMeasurable;
import org.apache.flume.serialization.RemoteMarkable;
import org.apache.flume.serialization.ResettableInputStream;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * A fake ResettableInputStream, it can not reset actually!
 * 
 * @author yukai
 *
 */
public class MockResettableInputStream extends ResettableInputStream implements RemoteMarkable, LengthMeasurable {
	private BufferedReader reader;

	public MockResettableInputStream(InputStream in, String charsetName) throws IOException {
		this.reader = new BufferedReader(new InputStreamReader(in, charsetName));
	}

	public MockResettableInputStream(InputStream in, String charsetName, int bufSize) throws IOException {
		this.reader = new BufferedReader(new InputStreamReader(in, charsetName), bufSize);
	}

	@Override
	public long length() throws IOException {
		return 0;
	}

	@Override
	public long getMarkPosition() throws IOException {
		return 0;
	}

	@Override
	public void markPosition(long arg0) throws IOException {

	}

	@Override
	public void close() throws IOException {
		this.reader.close();
	}

	@Override
	public void mark() throws IOException {

	}

	@Override
	public int read() throws IOException {
		throw new IOException("Unsupported Method.");
	}

	@Override
	public int read(byte[] b, int off, int len) throws IOException {
		throw new IOException("Unsupported Method.");
	}

	@Override
	public int readChar() throws IOException {
		return this.reader.read();
	}

	@Override
	public void reset() throws IOException {

	}

	@Override
	public void seek(long arg0) throws IOException {

	}

	@Override
	public long tell() throws IOException {
		return 0;
	}

}
