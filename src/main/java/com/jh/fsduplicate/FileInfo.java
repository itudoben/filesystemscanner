package com.jh.fsduplicate;

import java.sql.Timestamp;

/**
 * @author <font size=-1 color="#a3a3a3">Johnny Hujol</font>
 * @since 5/5/12
 */
public final class FileInfo {

  private final String hashDigest;

  private final String path;

  private final Timestamp timeComputed;

  FileInfo(String hashDigest, String path, Timestamp timeComputed) {
    this.hashDigest = hashDigest;
    this.path = path;
    this.timeComputed = timeComputed;
  }

  public String getHashDigest() {
    return hashDigest;
  }

  public String getPath() {
    return path;
  }

  public Timestamp getTimeComputed() {
    return timeComputed;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("FileInfo");
    sb.append("{hashDigest='").append(hashDigest).append('\'');
    sb.append(", path='").append(path).append('\'');
    sb.append(", timeComputed=").append(timeComputed);
    sb.append('}');
    return sb.toString();
  }
}
