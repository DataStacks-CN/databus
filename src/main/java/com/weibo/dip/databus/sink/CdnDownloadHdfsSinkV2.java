package com.weibo.dip.databus.sink;

import com.google.common.base.Preconditions;
import com.weibo.dip.data.platform.commons.util.GsonUtil;
import com.weibo.dip.databus.core.Configuration;
import com.weibo.dip.databus.core.Constants;
import com.weibo.dip.databus.core.Message;
import com.weibo.dip.databus.core.Sink;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.lang.time.FastDateFormat;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by jianhong1 on 2018/5/31.
 */
public class CdnDownloadHdfsSinkV2 extends Sink {
  private static final Logger LOGGER = LoggerFactory.getLogger(CdnDownloadHdfsSinkV2.class);
  // HDFS_BASE_DIR/category/day_and_hour/logName
  private static final String FILE_PATH_PATTERN = "%s/%s/%s/%s";
  private static final String HDFS_BASE_DIR = "sink.hdfs.base.dir";
  private static final String LOCAL_BASE_DIR = "sink.local.base.dir";
  private static final String HTTP_TIMEOUT = "sink.http.timeout.ms";
  private static final String CATEGORY = "Category";
  private static final String FILE_NAME = "FileName";
  private static final String FILE_URL = "FileUrl";
  private static final String TIMESTAMP = "Timestamp";
  private static final int RETRY_TIME = 3;
  private static FileSystem filesystem;
  private static FastDateFormat filePathDateFormat = FastDateFormat.getInstance("yyyy_MM_dd/HH");

  static {
    try {
      filesystem = FileSystem.get(new org.apache.hadoop.conf.Configuration());
    } catch (IOException e) {
      throw new ExceptionInInitializerError("init hdfs filesystem error: " + e);
    }
  }

  private String hdfsBaseDir;
  private String localBaseDir;
  private String httpTimeout;

  @Override
  public void process(Message message) throws Exception {
    String line = message.getData();
    if (StringUtils.isEmpty(line)) {
      LOGGER.warn("message data is empty, topic:{}", message.getTopic());
      return;
    }

    // 解析message
    Map<String, Object> map = GsonUtil.fromJson(line, GsonUtil.GsonType.OBJECT_MAP_TYPE);
    String category = map.get(CATEGORY).toString();
    String originFileName = map.get(FILE_NAME).toString();
    String fileUrl = map.get(FILE_URL).toString();
    long timestamp = ((Double) map.get(TIMESTAMP)).longValue();

    File localFile = new File(localBaseDir + "/" + originFileName);

    String fileName = originFileName;
    String codec = null;

    // 判断是否gz压缩文件
    if (".gz"
        .equals(originFileName.substring(originFileName.length() - 3, originFileName.length()))) {
      fileName = originFileName.substring(0, originFileName.length() - 3) + ".log";
      codec = "gz";
    } else {
      LOGGER.warn("not gz file {}", originFileName);
    }

    // 拼接hdfs文件路径
    String dayHour = filePathDateFormat.format(timestamp);
    String dotFileName = "." + fileName;
    String dotFilePath =
        String.format(FILE_PATH_PATTERN, hdfsBaseDir, category, dayHour, dotFileName);
    String filePath = String.format(FILE_PATH_PATTERN, hdfsBaseDir, category, dayHour, fileName);

    Path hdfsDotFile = new Path(dotFilePath);
    Path hdfsFile = new Path(filePath);

    // 若下载失败，则最多重试三次
    for (int index = 0; index < RETRY_TIME; index++) {
      try {
        // 下载http压缩文件到本地
        downloadToLocal(fileUrl, localFile);

        // 把本地文件解压缩、上传至hdfs
        uploadToHdfs(localFile, hdfsDotFile, hdfsFile, codec);

        return;
      } catch (Exception e) {
        LOGGER.error(
            "download httpfile or upload localfile fail, fileUrl: {}, retry time: {}, {}",
            fileUrl,
            index + 1,
            ExceptionUtils.getFullStackTrace(e));
      }
    }
  }

  private void uploadToHdfs(File localFile, Path hdfsDotFile, Path hdfsFile, String codec)
      throws IOException {
    // 把本地文件解压缩、上传至hdfs临时文件
    uploadAsDotFile(localFile, hdfsDotFile, codec);
    // 重命名hdfs临时文件为正式文件
    rename(hdfsDotFile, hdfsFile);
    LOGGER.info("upload to hdfs and rename completely, localfile: {}, hdfsfile: {}", localFile, hdfsFile);

    // 删除本地文件
    if (localFile.exists()) {
      Preconditions.checkState(localFile.delete(),
          "delete localfile:" + localFile + " failed");
      LOGGER.info("delete localfile:{} successfully", localFile);
    }
  }

  private void rename(Path hdfsDotFile, Path hdfsFile) throws IOException {
    if (filesystem.exists(hdfsFile)) {
      filesystem.delete(hdfsFile, true);
      LOGGER.info("delete existed hdfsfile: {}", hdfsFile);
    }
    filesystem.rename(hdfsDotFile, hdfsFile);
    LOGGER.debug("rename {} to {} completely", hdfsDotFile, hdfsFile);
  }

  private void uploadAsDotFile(File localFile, Path hdfsPath, String codec) throws IOException {
    InputStream inputStream = null;
    FSDataOutputStream outputStream = null;

    try {
      inputStream = new FileInputStream(localFile);
      if ("gz".equals(codec)) {
        inputStream = new GZIPInputStream(inputStream);
      }

      outputStream = filesystem.create(hdfsPath, true);

      LOGGER.debug("uploading localfile: {} to hdfs dotfile: {}", localFile, hdfsPath);
      IOUtils.copyBytes(inputStream, outputStream, 4096);
      LOGGER.debug(
          "upload localfile to hdfs dotfile completely, localfile: {}, hdfs dotfile: {}",
          localFile,
          hdfsPath);
    } finally {
      try {
        if (outputStream != null) {
          outputStream.close();
        }
      } finally {
        if (inputStream != null) {
          inputStream.close();
        }
      }
    }
  }

  private void downloadToLocal(String fileUrl, File localFile) throws Exception {
    CloseableHttpClient httpClient = HttpClients.createDefault();
    try {
      HttpGet httpGet = new HttpGet(fileUrl);

      //请求的参数配置，分别设置连接池获取连接的超时时间，连接上服务器的时间，服务器返回数据的时间
      RequestConfig requestConfig = RequestConfig.custom()
          .setConnectionRequestTimeout(Integer.parseInt(httpTimeout))
          .setConnectTimeout(Integer.parseInt(httpTimeout))
          .setSocketTimeout(Integer.parseInt(httpTimeout)).build();
      httpGet.setConfig(requestConfig);

      CloseableHttpResponse httpResponse = null;
      try {
        httpResponse = httpClient.execute(httpGet);
        int statusCode = httpResponse.getStatusLine().getStatusCode();
        LOGGER.info("http response status code: {}, httpfile: {}", statusCode, fileUrl);
        Preconditions.checkState(statusCode == 200,
            "http response status code:" + statusCode + ", not equal 200, httpfile:" + fileUrl);

        HttpEntity entity = httpResponse.getEntity();
        Preconditions.checkNotNull(entity,
            "the message entity of this response is null, httpfile:" + fileUrl);

        // 删除本地文件
        if (localFile.exists()) {
          Preconditions.checkState(localFile.delete(),
              "delete localfile:" + localFile + " failed");
          LOGGER.info("delete existed localfile:{} ", localFile);
        }

        InputStream inputStream = null;
        OutputStream outputStream = null;
        try {
          inputStream = entity.getContent();
          outputStream = new FileOutputStream(localFile);

          LOGGER.debug("downloading httpfile: {} to localfile: {}", fileUrl, localFile);
          IOUtils.copyBytes(inputStream, outputStream, 4096);

          // 文件大小校验
          long contentLength = entity.getContentLength();
          long fileLength = localFile.length();
          Preconditions.checkState(contentLength == fileLength,
              "file verify fail, httpfile size:" + contentLength
                  + "not equal localfile size:" + fileLength + ", httpfile:" + fileUrl);

          LOGGER.info(
              "download httpfile to localfile completely, httpfile:{}, localfile:{}, file size:{}",
              fileUrl,
              localFile,
              contentLength);
        } finally {
          try {
            if (outputStream != null) {
              outputStream.close();
            }
          } finally {
            if (inputStream != null) {
              inputStream.close();
            }
          }
        }
      } finally {
        if (httpResponse != null) {
          httpResponse.close();
        }
      }
    } finally {
      if (httpClient != null) {
        httpClient.close();
      }
    }
  }

  @Override
  public void setConf(Configuration conf) {
    name = conf.get(Constants.PIPELINE_NAME) + Constants.HYPHEN + this.getClass().getSimpleName();

    hdfsBaseDir = conf.get(HDFS_BASE_DIR);
    Preconditions.checkState(
        StringUtils.isNotEmpty(hdfsBaseDir), name + " " + HDFS_BASE_DIR + " must be specified");
    LOGGER.info("Property: {}={}", HDFS_BASE_DIR, hdfsBaseDir);

    localBaseDir = conf.get(LOCAL_BASE_DIR);
    Preconditions.checkState(
        StringUtils.isNotEmpty(localBaseDir), name + " " + LOCAL_BASE_DIR + " must be specified");
    LOGGER.info("Property: {}={}", LOCAL_BASE_DIR, localBaseDir);

    httpTimeout = conf.get(HTTP_TIMEOUT);
    Preconditions.checkState(StringUtils.isNumeric(httpTimeout),
        name + " " + HTTP_TIMEOUT + " must be numeric");
    LOGGER.info("Property: {}={}", HTTP_TIMEOUT, httpTimeout);
  }

  @Override
  public void start() {
    LOGGER.info("{} started", name);
  }

  @Override
  public void stop() {
    LOGGER.info("{} stopped", name);
  }
}
