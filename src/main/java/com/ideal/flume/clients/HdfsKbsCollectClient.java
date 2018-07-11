package com.ideal.flume.clients;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.security.UserGroupInformation;

import com.google.common.base.Preconditions;
import com.ideal.flume.file.CollectFile;
import com.ideal.flume.file.CollectFileFilter;
import com.ideal.flume.file.HdfsCollectFile;

public class HdfsKbsCollectClient implements CollectClient {
    private final String ticketCache;
    private final String keytab;
    private final String principal;

    private UserGroupInformation ugi;
    private final Configuration config = new Configuration();

    public HdfsKbsCollectClient(String ticketCache, String keytab, String principal)
            throws Exception {
        if (StringUtils.isBlank(principal)
                && (StringUtils.isBlank(ticketCache) || StringUtils.isBlank(keytab))) {
            throw new IllegalArgumentException(
                    "principal is required, and ticketCache or keytab is required.");
        }

        this.ticketCache = ticketCache;
        this.keytab = keytab;
        this.principal = principal;
        getClient();
    }

    private UserGroupInformation getClient() throws Exception {
        if (ugi == null) {
            ugi = createClient();
        }

        return ugi;
    }

    private UserGroupInformation createClient() throws Exception {
        if (StringUtils.isNotBlank(keytab)) {
            ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytab);
        } else {
            ugi = UserGroupInformation.getUGIFromTicketCache(ticketCache, principal);
        }
        return ugi;
    }

    @Override
    public boolean isConnected() throws Exception {
        return ugi != null;
    }

    @Override
    public void disconnect() throws Exception {
        ugi = null;
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

    private List<CollectFile> doListFiles(UserGroupInformation ugi, final String pathname,
            final CollectFileFilter filter, final boolean recursive) throws Exception {
        final List<CollectFile> ret = new ArrayList<CollectFile>();
        RemoteIterator<LocatedFileStatus> it =
                ugi.doAs(new PrivilegedExceptionAction<RemoteIterator<LocatedFileStatus>>() {
                    @Override
                    public RemoteIterator<LocatedFileStatus> run() throws Exception {
                        Path p = new Path(pathname);
                        FileSystem fs = p.getFileSystem(config);
                        return fs.listFiles(p, recursive);
                    }
                });

        while (it.hasNext()) {
            LocatedFileStatus lfs = it.next();
            HdfsCollectFile hcf = new HdfsCollectFile(lfs.getPath(), lfs);
            if (filter.accept(hcf))
                ret.add(hcf);
        }

        return ret;
    }

    @Override
    public void deleteFile(final String relPath) throws Exception {
        PrivilegedExceptionAction<Boolean> action = new DeleteAction(new Path(relPath));

        try {
            getClient().doAs(action);
        } catch (Exception e) {
            disconnect();

            getClient().doAs(action);
        }
    }

    @Override
    public boolean rename(final String oldName, final String newName) throws Exception {
        final Path dst = new Path(newName);

        Path dstParent = dst.getParent();
        if (null == dstParent || !mkdirs(dstParent)) {
            throw new RuntimeException("cann not creat dirs " + dstParent.toUri().getPath());
        }

        PrivilegedExceptionAction<Boolean> action = new RenameAction(new Path(oldName), dst);

        try {
            return getClient().doAs(action);
        } catch (Exception e) {
            disconnect();

            return getClient().doAs(action);
        }
    }

    @Override
    public InputStream retrieveFileStream(final String relPath) throws Exception {
        PrivilegedExceptionAction<InputStream> action = new RetrieveAction(new Path(relPath));

        try {
            return getClient().doAs(action);
        } catch (Exception e) {
            disconnect();

            return getClient().doAs(action);
        }
    }

    @Override
    public OutputStream create(String relPath) throws Exception {
        final Path file = new Path(relPath);
        Preconditions.checkState(file.isAbsolute(), "The file must be in an absolute path.");

        PrivilegedExceptionAction<OutputStream> action = new CreateAction(file);

        try {
            return getClient().doAs(action);
        } catch (Exception e) {
            disconnect();

            return getClient().doAs(action);
        }
    }

    @Override
    public boolean exists(final String relPath) throws Exception {
        PrivilegedExceptionAction<Boolean> action = new ExistsAction(new Path(relPath));

        try {
            return getClient().doAs(action);
        } catch (Exception e) {
            disconnect();

            return getClient().doAs(action);
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

    @Override
    public boolean mkdirs(final String path) throws Exception {
        PrivilegedExceptionAction<Boolean> action = new MkdirsAction(new Path(path));

        try {
            return getClient().doAs(action);
        } catch (Exception e) {
            disconnect();

            return getClient().doAs(action);
        }
    }

    private boolean mkdirs(Path path) throws Exception {
        PrivilegedExceptionAction<Boolean> action = new MkdirsAction(path);

        try {
            return getClient().doAs(action);
        } catch (Exception e) {
            disconnect();

            return getClient().doAs(action);
        }
    }

    class MkdirsAction implements PrivilegedExceptionAction<Boolean> {
        public final Path path;

        public MkdirsAction(Path path) {
            this.path = path;
        }

        @Override
        public Boolean run() throws Exception {
            FileSystem fs = path.getFileSystem(config);
            return fs.mkdirs(path);
        }
    }

    class ExistsAction implements PrivilegedExceptionAction<Boolean> {
        public final Path path;

        public ExistsAction(Path path) {
            this.path = path;
        }

        @Override
        public Boolean run() throws Exception {
            FileSystem fs = path.getFileSystem(config);
            return fs.exists(path);
        }
    }

    class CreateAction implements PrivilegedExceptionAction<OutputStream> {
        public final Path path;

        public CreateAction(Path path) {
            this.path = path;
        }

        @Override
        public OutputStream run() throws Exception {
            FileSystem fs = path.getFileSystem(config);
            return fs.create(path, true);
        }
    }

    class DeleteAction implements PrivilegedExceptionAction<Boolean> {
        public final Path path;

        public DeleteAction(Path path) {
            this.path = path;
        }

        @Override
        public Boolean run() throws Exception {
            FileSystem fs = path.getFileSystem(config);
            return fs.delete(path, false);
        }
    }

    class RetrieveAction implements PrivilegedExceptionAction<InputStream> {
        public final Path path;

        public RetrieveAction(Path path) {
            this.path = path;
        }

        @Override
        public InputStream run() throws Exception {
            FileSystem fs = path.getFileSystem(config);
            return fs.open(path);
        }
    }

    class RenameAction implements PrivilegedExceptionAction<Boolean> {
        public final Path srcPath;
        public final Path dstPath;

        public RenameAction(Path srcPath, Path dstPath) {
            this.srcPath = srcPath;
            this.dstPath = dstPath;
        }

        @Override
        public Boolean run() throws Exception {
            FileSystem fs = srcPath.getFileSystem(config);
            return fs.rename(srcPath, dstPath);
        }
    }

}
