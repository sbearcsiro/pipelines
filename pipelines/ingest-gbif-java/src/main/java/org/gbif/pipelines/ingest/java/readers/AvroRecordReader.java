package org.gbif.pipelines.ingest.java.readers;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.gbif.pipelines.io.avro.Record;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.AVRO_EXTENSION;

/** Avro format reader, reads {@link Record} based objects using sting or {@link List<File>} path */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class AvroRecordReader {

  /**
   * Read {@link Record#getId()} unique records
   *
   * @param clazz instance of {@link Record}
   * @param path sting path, a wildcard can be used in the file name, like /a/b/c*.avro to read multiple files
   */
  public static <T extends Record> Map<String, T> readUniqueRecords(Class<T> clazz, String path) {
    List<File> paths = parseWildcardPath(path);
    return readUniqueRecords(clazz, paths);
  }

  /**
   * Read {@link Record#getId()} unique records
   *
   * @param clazz instance of {@link Record}
   * @param paths list of paths to the files
   */
  @SneakyThrows
  public static <T extends Record> Map<String, T> readUniqueRecords(Class<T> clazz, List<File> paths) {

    Map<String, T> map = new HashMap<>();
    Set<String> duplicateSet = new HashSet<>();

    for (File path : paths) {
      // Deserialize avro record from disk
      DatumReader<T> reader = new SpecificDatumReader<>(clazz);
      try (DataFileReader<T> dataFileReader = new DataFileReader<>(path, reader)) {
        while (dataFileReader.hasNext()) {
          T next = dataFileReader.next();

          T saved = map.get(next.getId());
          if (saved == null && !duplicateSet.contains(next.getId())) {
            map.put(next.getId(), next);
          } else if (saved != null && !saved.equals(next)) {
            map.remove(next.getId());
            duplicateSet.add(next.getId());
            log.warn("occurrenceId = {}, duplicates were found", saved.getId());
          }

        }
      }
    }

    return map;
  }

  /**
   * Read {@link Record#getId()} distinct records
   *
   * @param clazz instance of {@link Record}
   * @param path sting path, a wildcard can be used in the file name, like /a/b/c*.avro to read multiple files
   */
  public static <T extends Record> Map<String, T> readRecords(Class<T> clazz, String path) {
    List<File> paths = parseWildcardPath(path);
    return readRecords(clazz, paths);
  }

  /**
   * Read {@link Record#getId()} distinct records
   *
   * @param clazz instance of {@link Record}
   * @param paths list of paths to the files
   */
  @SneakyThrows
  public static <T extends Record> Map<String, T> readRecords(Class<T> clazz, List<File> paths) {

    Map<String, T> map = new HashMap<>();

    for (File path : paths) {
      // Deserialize ExtendedRecord from disk
      DatumReader<T> reader = new SpecificDatumReader<>(clazz);
      try (DataFileReader<T> dataFileReader = new DataFileReader<>(path, reader)) {
        while (dataFileReader.hasNext()) {
          T next = dataFileReader.next();
          map.put(next.getId(), next);
        }
      }
    }

    return map;
  }

  /** Read multiple files, with the wildcard in the path */
  private static List<File> parseWildcardPath(String path) {
    if (path.contains("*")) {
      File parentFile = new File(path).getParentFile();
      if (parentFile.listFiles() != null) {
        return Arrays.stream(parentFile.listFiles())
            .filter(f -> f.isFile() && f.toString().endsWith(AVRO_EXTENSION))
            .collect(Collectors.toList());
      }
    }
    return Collections.singletonList(new File(path));
  }

}
