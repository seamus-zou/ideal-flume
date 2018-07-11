package com.ideal.flume.clients;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Preconditions;
import com.ideal.flume.file.CollectFile;
import com.ideal.flume.file.CollectFileFilter;
import com.ideal.flume.file.HdfsCollectFile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HdfsCollectClient implements CollectClient {
    private static final Logger logger = LoggerFactory.getLogger(HdfsCollectClient.class);

    private final Configuration confHdfs;
    private FileSystem fileSystem;

    public HdfsCollectClient(Configuration confHdfs) throws Exception {
        this.confHdfs = confHdfs;
        getClient();
    }

    private FileSystem getClient() throws Exception {
        if (fileSystem == null) {
            fileSystem = createClient();
        }

        return fileSystem;
    }

    private FileSystem createClient() throws Exception {
        return FileSystem.newInstance(confHdfs);
    }

    @Override
    public boolean isConnected() throws Exception {
        return fileSystem != null;
    }

    @Override
    public void disconnect() throws Exception {
        try {
            if (null != fileSystem)
                fileSystem.close();
        } finally {
            fileSystem = null;
        }
    }

    @Override
    public List<CollectFile> listFiles(String pathname, CollectFileFilter filter) throws Exception {
        return listFiles(pathname, filter, false);
    }

    @Override
    public List<CollectFile> listFiles(String pathname, CollectFileFilter filter, boolean recursive)
            throws Exception {
        try {
            return doListFiles(getClient(), pathname, filter, recursive);
        } catch (Exception e) {
            disconnect();

            return doListFiles(getClient(), pathname, filter, recursive);
        }
    }

    private List<CollectFile> doListFiles(FileSystem client, String pathname,
            final CollectFileFilter filter, final boolean recursive) throws Exception {
        final List<CollectFile> ret = new ArrayList<CollectFile>();
        RemoteIterator<LocatedFileStatus> it = client.listFiles(new Path(pathname), recursive);
        while (it.hasNext()) {
            LocatedFileStatus lfs = it.next();
            HdfsCollectFile hcf = new HdfsCollectFile(lfs.getPath(), lfs);
            if (filter.accept(hcf))
                ret.add(hcf);
        }
        return ret;
    }

    @Override
    public void deleteFile(String relPath) throws Exception {
        try {
            getClient().delete(new Path(relPath), false);
        } catch (Exception e) {
            disconnect();
            getClient().delete(new Path(relPath), false);
        }
    }

    @Override
    public boolean rename(String oldName, String newName) throws Exception {
        Path dst = new Path(newName);

        Path dstParent = dst.getParent();
        logger.info(dstParent.toUri().getPath());
        if (null == dstParent || !mkdirs(dstParent)) {
            throw new RuntimeException("cann not creat dirs " + dstParent.toUri().getPath());
        }

        try {
            return getClient().rename(new Path(oldName), dst);
        } catch (Exception e) {
            disconnect();
            return getClient().rename(new Path(oldName), dst);
        }
    }

    @Override
    public InputStream retrieveFileStream(String relPath) throws Exception {
        try {
            return getClient().open(new Path(relPath));
        } catch (IOException e) {
            disconnect();
            return getClient().open(new Path(relPath));
        }
    }

    @Override
    public OutputStream create(String relPath) throws Exception {
        Path file = new Path(relPath);
        Preconditions.checkState(file.isAbsolute(), "The file must be in an absolute path.");

        try {
            return getClient().create(file, true);
        } catch (IOException e) {
            disconnect();
            return getClient().create(file, true);
        }
    }

    @Override
    public boolean exists(String relPath) throws Exception {
        try {
            return exists(new Path(relPath));
        } catch (Exception e) {
            disconnect();
            return exists(new Path(relPath));
        }
    }

    private boolean exists(Path parh) throws Exception {
        try {
            return getClient().exists(parh);
        } catch (IOException e) {
            disconnect();
            return getClient().exists(parh);
        }
    }

    @Override
    public boolean abort() throws Exception {
        try {
            disconnect();
            return true;
        } catch (IOException e) {
            disconnect();
        }
        return true;
    }

    @Override
    public void completePendingCommand() throws Exception {
        // do nothing
    }

    public Configuration getConfHdfs() {
        return confHdfs;
    }

    @Override
    public boolean mkdirs(String path) throws Exception {
        return mkdirs(new Path(path));
    }

    private boolean mkdirs(Path path) throws Exception {
        try {
            return getClient().mkdirs(path);
        } catch (Exception e) {
            disconnect();

            return getClient().mkdirs(path);
        }
    }

}
