package org.gbif.pipelines.fragmenter;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.gbif.converters.parser.xml.OccurrenceParser;
import org.gbif.converters.parser.xml.parsing.extendedrecord.ParserFileUtils;
import org.gbif.converters.parser.xml.parsing.validators.UniquenessValidator;
import org.gbif.pipelines.fragmenter.common.FragmentsConfig;
import org.gbif.pipelines.fragmenter.common.HbaseStore;
import org.gbif.pipelines.fragmenter.common.RecordUnit;
import org.gbif.pipelines.fragmenter.common.RecordUnitConverter;
import org.gbif.pipelines.keygen.HBaseLockingKeyService;
import org.gbif.pipelines.keygen.common.HbaseConnectionFactory;
import org.gbif.pipelines.keygen.config.KeygenConfig;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;

import lombok.Builder;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Builder
public class XmlFragmentsUploader {

  private static final String EXT_RESPONSE = ".response";
  private static final String EXT_XML = ".xml";

  @NonNull
  private FragmentsConfig config;

  @NonNull
  private KeygenConfig keygenConfig;

  @NonNull
  private Path pathToArchive;

  @NonNull
  private String datasetId;

  @NonNull
  private Integer attempt;

  @NonNull
  Boolean useTriplet;

  @NonNull
  Boolean useOccurrenceId;

  @Builder.Default
  private boolean useSyncMode = true;

  @Builder.Default
  private ExecutorService executor = Executors.newSingleThreadExecutor();

  @Builder.Default
  private int backPressure = 5;

  private Connection hbaseConnection;

  @SneakyThrows
  public long upload() {

    Connection connection = Optional.ofNullable(hbaseConnection)
        .orElse(HbaseConnectionFactory.getInstance(keygenConfig.getHbaseZk()).getConnection());
    HBaseLockingKeyService keygenService = new HBaseLockingKeyService(keygenConfig, connection, datasetId);

    List<CompletableFuture<Void>> futures = new ArrayList<>();
    AtomicInteger backPressureCounter = new AtomicInteger(0);
    AtomicInteger occurrenceCounter = new AtomicInteger(0);

    try (Table table = connection.getTable(config.getTableName());
        UniquenessValidator validator = UniquenessValidator.getNewInstance()) {

      Consumer<List<RecordUnit>> convertAndPushBulkFn = l -> {
        backPressureCounter.incrementAndGet();

        Map<Long, String> map = RecordUnitConverter.convert(keygenService, validator, useTriplet, useOccurrenceId, l);
        HbaseStore.putList(table, datasetId, attempt, map);

        occurrenceCounter.addAndGet(map.size());
        backPressureCounter.decrementAndGet();
      };

      Consumer<List<RecordUnit>> convertAndPushFn = l ->
          Optional.ofNullable(l)
              .filter(req -> !req.isEmpty())
              .ifPresent(req -> {
                if (useSyncMode) {
                  convertAndPushBulkFn.accept(req);
                } else {
                  futures.add(CompletableFuture.runAsync(() -> convertAndPushBulkFn.accept(req), executor));
                }
              });

      File inputFile = ParserFileUtils.uncompressAndGetInputFile(pathToArchive.toString());
      for (File f : getInputFiles(inputFile)) {

        while (backPressureCounter.get() > backPressure) {
          log.info("Back pressure barrier: too many rows wainting...");
          TimeUnit.MILLISECONDS.sleep(200L);
        }

        List<RecordUnit> recordUnitList = new OccurrenceParser().parseFile(f).stream()
            .map(RecordUnit::create)
            .collect(Collectors.toList());
        convertAndPushFn.accept(recordUnitList);
      }

      // Wait for all async jobs
      if (!useSyncMode) {
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
      }

    }

    return occurrenceCounter.get();
  }

  /** Traverse the input directory and gets all the files. */
  private List<File> getInputFiles(File inputhFile) throws IOException {
    Predicate<Path> prefixPr = x -> x.toString().endsWith(EXT_RESPONSE) || x.toString().endsWith(EXT_XML);
    try (Stream<Path> walk = Files.walk(inputhFile.toPath())
        .filter(file -> file.toFile().isFile() && prefixPr.test(file))) {
      return walk.map(Path::toFile).collect(Collectors.toList());
    }
  }

}