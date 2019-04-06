package org.jh.filesystemscanner.core;

import java.sql.Timestamp;

/**
 * @author <font size=-1 color="#a3a3a3">Johnny Hujol</font>
 * @since 5/5/12
 */
public final class FileInfo {

    private final String hashDigest;

    private final long size;

    private final String path;

    private final String extension;

    private final Timestamp timeComputed;

    public FileInfo(String hashDigest, String path, Timestamp timeComputed, long size) {
        this.hashDigest = hashDigest;
        this.path = path;
        String[] elts = path.split("\\.");
        this.extension = elts[elts.length - 1];
        this.timeComputed = timeComputed;
        this.size = size;
    }

    String getHashDigest() {
        return hashDigest;
    }

    public String getPath() {
        return path;
    }

    public Timestamp getTimeComputed() {
        return timeComputed;
    }

    String getExtension() {
        return extension;
    }

    long getSize() {
        return size;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("FileInfo");
        sb.append("{hashDigest='").append(hashDigest).append('\'');
        sb.append(", path='").append(path).append('\'');
        sb.append(", extension='").append(extension).append('\'');
        sb.append(", size='").append(size).append('\'');
        sb.append(", timeComputed=").append(timeComputed);
        sb.append('}');
        return sb.toString();
    }
}
