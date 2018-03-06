package org.gbif.pipelines.labs;

import org.gbif.pipelines.io.avro.Event;
import org.gbif.pipelines.io.avro.ExtendedOccurrence;
import org.gbif.pipelines.io.avro.Location;
import org.gbif.pipelines.common.beam.Coders;
import org.gbif.pipelines.common.beam.DwCAIO;
import org.gbif.pipelines.io.avro.Issue;
import org.gbif.pipelines.io.avro.IssueLineageRecord;
import org.gbif.pipelines.io.avro.Lineage;
import org.gbif.pipelines.config.DataPipelineOptionsFactory;
import org.gbif.pipelines.config.DataProcessingPipelineOptions;
import org.gbif.pipelines.config.OptionsKeyEnum;
import org.gbif.pipelines.config.TargetPath;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.InterpretedExtendedRecord;
import org.gbif.pipelines.transform.common.Kv2Value;
import org.gbif.pipelines.transform.record.InterpretedExtendedRecordTransform;
import org.gbif.pipelines.transform.validator.UniqueOccurrenceIdTransform;

import java.util.Map;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple demonstration showing a pipeline running locally which will read UntypedOccurrence from a DwC-A file
 * transform it into interpreted occurence records
 * .
 * <p>
 * Run it
 * On LocalFileSystem
 * mvn compile exec:java -Dexec.mainClass=org.gbif.pipelines.demo.DwCA2InterpretedRecordsPipeline -Dexec.args="--datasetId=abc123 --inputFile=data/dwca.zip" -Pdirect-runner
 * On HDFS
 * mvn compile exec:java -Dexec.mainClass=org.gbif.pipelines.demo.DwCA2InterpretedRecordsPipeline -Dexec.args="--datasetId=abc123 --inputFile=data/dwca.zip --HDFSConfigurationDirectory=/path/to/hadoop-conf/ --defaultTargetDirectory=hdfs://ha-nn/user/hive/warehouse/gbif-data/abc123/" -Pdirect-runner
 */
public class DwCA2InterpretedRecordsPipeline {

  private static final Logger LOG = LoggerFactory.getLogger(DwCA2InterpretedRecordsPipeline.class);

  public static void main(String[] args) {

    // STEP 0: Configure pipeline
    DataProcessingPipelineOptions options = DataPipelineOptionsFactory.create(args);
    Map<OptionsKeyEnum, TargetPath> targetPaths = options.getTargetPaths();

    Pipeline p = Pipeline.create(options);

    Coders.registerAvroCoders(p, ExtendedRecord.class, Event.class, Location.class, ExtendedOccurrence.class);
    Coders.registerAvroCoders(p, Issue.class, Lineage.class, IssueLineageRecord.class);

    // STEP 1: Read the DwC-A using our custom reader
    PCollection<ExtendedRecord> rawRecords = p.apply("Read from Darwin Core Archive",
                                                     DwCAIO.Read.withPaths(options.getInputFile(),
                                                                           targetPaths.get(OptionsKeyEnum
                                                                                             .TEMP_DWCA_PATH)
                                                                             .filePath()));

    // STEP 2: Filter unique records by OccurrenceId
    UniqueOccurrenceIdTransform uniqueTransform = new UniqueOccurrenceIdTransform();
    PCollectionTuple uniqueTuple = rawRecords.apply(uniqueTransform);
    PCollection<ExtendedRecord> uniqueRecords = uniqueTuple.get(uniqueTransform.getDataTag());

    // STEP 3: Write records in an avro file, this will be location of the hive table which has raw records
    uniqueRecords.apply("Save the interpreted records as Avro",
                        AvroIO.write(ExtendedRecord.class).to(targetPaths.get(OptionsKeyEnum.RAW_OCCURRENCE).filePath()));

    // STEP 4: Interpret the raw records as a tuple, which has both different categories of data and issue related to them
    InterpretedExtendedRecordTransform extendedRecordTransform = new InterpretedExtendedRecordTransform();
    PCollectionTuple extendedRecordsTuple = uniqueRecords.apply(extendedRecordTransform);
    PCollection<InterpretedExtendedRecord> extendedRecords = extendedRecordsTuple.get(extendedRecordTransform.getDataTag())
        .apply(Kv2Value.create());

    // STEP 5: writing interpreted occurence and issues to the avro file
    extendedRecords.apply("Save the processed records as Avro",
                          AvroIO.write(InterpretedExtendedRecord.class)
                            .to(targetPaths.get(OptionsKeyEnum.INTERPRETED_OCURENCE).filePath()));

    LOG.info("Starting the pipeline");
    PipelineResult result = p.run();
    result.waitUntilFinish();
    LOG.info("Pipeline finished with state: {} ", result.getState());
  }

}