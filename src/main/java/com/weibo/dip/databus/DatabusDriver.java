package com.weibo.dip.databus;

import com.google.common.base.Preconditions;
import com.weibo.dip.databus.core.Configuration;
import com.weibo.dip.databus.core.Constants;
import com.weibo.dip.databus.core.Metric;
import com.weibo.dip.databus.core.Pipeline;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;

/**
 * Created by yurun on 17/8/9.
 */
public class DatabusDriver {

  private static final Logger LOGGER = LoggerFactory.getLogger(DatabusDriver.class);

  public static void main(String[] args) {
    Supervisor supervisor;

    try {
      Configuration databusConf = new Configuration(
          DatabusDriver.class.getClassLoader().getResourceAsStream(Constants.DATABUS_PROPERTIES));

      supervisor = new Supervisor(databusConf);

      supervisor.start();
    } catch (Exception e) {
      LOGGER.error("Databus server start error: {}", ExceptionUtils.getStackTrace(e));

      return;
    }

    try {
      supervisor.join();
    } catch (InterruptedException e) {
      LOGGER.error("Supervisor may be running, but interrupted");
    }

    LOGGER.info("Databus server stopped");
  }

  private static class Supervisor extends Thread {

    private Configuration databusConf;

    private Map<File, Pipeline> pipelines = new HashMap<>();

    private Supervisor(Configuration databusConf) {
      this.databusConf = databusConf;
    }

    private void startPipeline(File pipelineConf) {
      try {
        Configuration configuration = new Configuration(pipelineConf);

        Pipeline pipeline = new Pipeline(pipelineConf.lastModified());

        pipeline.setConf(configuration);

        pipeline.start();

        pipelines.put(pipelineConf, pipeline);
      } catch (Exception e) {
        LOGGER.error("start pipeline with " + pipelineConf.getPath() + " error: "
            + ExceptionUtils.getStackTrace(e));
      }
    }

    private void stopPipeline(File pipelineConf) {
      if (!pipelines.containsKey(pipelineConf)) {
        return;
      }

      Pipeline pipeline = pipelines.remove(pipelineConf);

      try {
        pipeline.stop();
      } catch (Exception e) {
        LOGGER.error("stop pipeline with " + pipelineConf.getPath() + " error: "
            + ExceptionUtils.getStackTrace(e));
      }
    }

    private void stopAllPipelines() {
      if (MapUtils.isEmpty(pipelines)) {
        return;
      }

      for (Pipeline pipeline : pipelines.values()) {
        pipeline.stop();
      }

      pipelines.clear();
    }

    @Override
    public void run() {
      LOGGER.info("Supervisor started");

      try {
        /*
           pipeline conf
        */
        String pipelineConfPath = databusConf.get(Constants.PIPELINE_CONF);
        LOGGER.info("pipeline conf dir: {}", pipelineConfPath);
        Preconditions.checkState(StringUtils.isNotEmpty(pipelineConfPath),
            Constants.PIPELINE_CONF + " must be specified in " + Constants.DATABUS_PROPERTIES);

        File confDir = new File(pipelineConfPath);

        /*
            stopfile
         */
        String stopFilePath = databusConf.get(Constants.DATABUS_STOPFILE);
        LOGGER.info("stop file: {}", stopFilePath);
        Preconditions.checkState(StringUtils.isNotEmpty(stopFilePath),
            Constants.DATABUS_STOPFILE + " must be specified in " + Constants.DATABUS_PROPERTIES);

        File stopFile = new File(stopFilePath);

        /*
            metric initialize
         */
        Metric metric = Metric.getInstance();
        metric.setConf(databusConf);

        metric.start();

        while (!stopFile.exists()) {
          /*
              start new pipeline
              restart expired pipeline
           */
          Collection<File> pipelineConfs = null;

          if (confDir.exists() && confDir.isDirectory()) {
            pipelineConfs = FileUtils.listFiles(confDir, new String[] {Constants.PROPERTIES},
                true);

            if (CollectionUtils.isNotEmpty(pipelineConfs)) {
              for (File pipelineConf : pipelineConfs) {
                Pipeline pipeline = pipelines.get(pipelineConf);

                if (Objects.isNull(pipeline)) {
                  /*
                      pipeline not exist, start
                   */
                  LOGGER.info("pipeline config {} not exist, load it");
                  startPipeline(pipelineConf);
                } else {
                  if (pipelineConf.lastModified() > pipeline.getTimestamp()) {
                    /*
                        pipeline exist, but timestamp expired, restart
                     */
                    LOGGER.info("pipeline config {} expired, reload it");
                    stopPipeline(pipelineConf);
                    startPipeline(pipelineConf);
                  }
                }
              }
            } else {
              LOGGER.warn("databus conf dir doesn't have any pipeline properties");
            }
          } else {
            LOGGER.warn("databus conf dir doesn't exist or it's not a directory");
          }

          /*
              remove disused pipeline
           */
          List<File> disusedPipelineConfs = new ArrayList<>();

          for (File conf : pipelines.keySet()) {
            if (CollectionUtils.isEmpty(pipelineConfs) || !pipelineConfs.contains(conf)) {
              disusedPipelineConfs.add(conf);
            }
          }

          if (CollectionUtils.isNotEmpty(disusedPipelineConfs)) {
            for (File disusedPipelineConf : disusedPipelineConfs) {
              LOGGER.info("pipeline config {} unused, unload it", disusedPipelineConf);
              stopPipeline(disusedPipelineConf);
            }
          }

          try {
            Thread.sleep(Constants.SUPERVISOR_SLEEP);
          } catch (InterruptedException e) {
            LOGGER.warn("Supervisor sleep interrupted");

            break;
          }
        }

        LOGGER.info("stop file " + stopFile + " detected");

        stopAllPipelines();

        metric.stop();
      } catch (Exception e) {
        LOGGER.error("Supervisor run error: {}", ExceptionUtils.getStackTrace(e));
      }

      LOGGER.info("Supervisor stopped");
    }

  }

}
