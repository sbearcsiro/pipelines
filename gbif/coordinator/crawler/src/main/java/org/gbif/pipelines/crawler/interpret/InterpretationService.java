package org.gbif.pipelines.crawler.interpret;

import com.google.common.util.concurrent.AbstractIdleService;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.gbif.common.messaging.DefaultMessagePublisher;
import org.gbif.common.messaging.MessageListener;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage;
import org.gbif.pipelines.common.configs.StepConfiguration;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryWsClient;

/**
 * A service which listens to the {@link
 * org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage } and perform interpretation
 */
@Slf4j
public class InterpretationService extends AbstractIdleService {

  private final InterpreterConfiguration config;
  private MessageListener listener;
  private MessagePublisher publisher;
  private CuratorFramework curator;
  private CloseableHttpClient httpClient;
  private ExecutorService executor;

  public InterpretationService(InterpreterConfiguration config) {
    this.config = config;
  }

  @Override
  protected void startUp() throws Exception {
    log.info("Started pipelines-interpret-dataset service with parameters : {}", config);
    // Prefetch is one, since this is a long-running process.
    StepConfiguration c = config.stepConfig;
    listener = new MessageListener(c.messaging.getConnectionParameters(), 1);
    publisher = new DefaultMessagePublisher(c.messaging.getConnectionParameters());
    curator = c.zooKeeper.getCuratorFramework();
    executor =
        config.standaloneNumberThreads == null
            ? null
            : Executors.newFixedThreadPool(config.standaloneNumberThreads);

    PipelinesHistoryWsClient historyWsClient =
        c.registry.newRegistryInjector().getInstance(PipelinesHistoryWsClient.class);
    httpClient =
        HttpClients.custom()
            .setDefaultRequestConfig(
                RequestConfig.custom().setConnectTimeout(60_000).setSocketTimeout(60_000).build())
            .build();

    InterpretationCallback callback =
        new InterpretationCallback(
            config, publisher, curator, historyWsClient, httpClient, executor);

    String validator = config.validatorOnly ? ".validator." : ".";
    String routingKey =
        PipelinesVerbatimMessage.ROUTING_KEY + validator + config.processRunner.toLowerCase();
    listener.listen(c.queueName, routingKey, c.poolSize, callback);
  }

  @Override
  protected void shutDown() {
    listener.close();
    publisher.close();
    curator.close();
    executor.shutdown();
    try {
      httpClient.close();
    } catch (IOException e) {
      log.error("Can't close ES http client connection");
    }
    log.info("Stopping pipelines-interpret-dataset service");
  }
}
