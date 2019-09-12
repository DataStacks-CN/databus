package com.weibo.dip.databus.source;

/**
 * Created by jianhong1 on 2019-07-09.
 */
public class FileStatus {
  private String path;
  private long offset;
  private boolean isCompleted;

  public FileStatus() {
  }

  public FileStatus(String path) {
    this.path = path;
  }

  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }

  public long getOffset() {
    return offset;
  }

  public void setOffset(long offset) {
    this.offset = offset;
  }

  public boolean isCompleted() {
    return isCompleted;
  }

  public void setCompleted(boolean completed) {
    isCompleted = completed;
  }

  @Override
  public String toString() {
    return path + " " + offset + " " + isCompleted;
  }
}
