package au.org.ala.pipelines.beam;

import au.org.ala.util.TestUtils;
import java.io.File;
import java.io.Serializable;
import java.util.function.Function;
import org.apache.commons.io.FileUtils;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.ingest.options.DwcaPipelineOptions;
import org.gbif.pipelines.ingest.options.InterpretationPipelineOptions;
import org.gbif.pipelines.ingest.options.PipelinesOptionsFactory;
import org.gbif.pipelines.ingest.pipelines.DwcaToVerbatimPipeline;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.junit.Test;

/** End to end default values test. */
public class DefaultValuesTestIT {

  @Test
  public void testDwCaPipeline() throws Exception {

    // clear up previous test runs
    FileUtils.deleteQuietly(new File("/tmp/la-pipelines-test/default-values"));

    String absolutePath = new File("src/test/resources").getAbsolutePath();

    // Load test DwC archive
    DwcaPipelineOptions dwcaOptions =
        PipelinesOptionsFactory.create(
            DwcaPipelineOptions.class,
            new String[] {
              "--datasetId=dr893",
              "--attempt=1",
              "--runner=DirectRunner",
              "--metaFileName=dwca-metrics.yml",
              "--targetPath=/tmp/la-pipelines-test/default-values",
              "--inputPath=" + absolutePath + "/default-values/dr893"
            });
    DwcaToVerbatimPipeline.run(dwcaOptions);

    // check the original verbatim values are NOT populated with default values
    InterpretationPipelineOptions testOptions1 =
        PipelinesOptionsFactory.create(
            InterpretationPipelineOptions.class,
            new String[] {
              "--datasetId=dr893",
              "--attempt=1",
              "--runner=DirectRunner",
              "--metaFileName=uuid-metrics.yml",
              "--targetPath=/tmp/la-pipelines-test/default-values",
              "--inputPath=/tmp/la-pipelines-test/default-values/dr893/1/verbatim.avro",
              "--properties=" + TestUtils.getPipelinesConfigFile(),
              "--useExtendedRecordId=true"
            });

    // validate that the raw values for basisOfRecord and occurrenceStatus are current null
    Function<ExtendedRecord, Boolean> notPopulated =
        (Function<ExtendedRecord, Boolean> & Serializable)
            er ->
                er.getCoreTerms()
                            .get(DwcTerm.basisOfRecord.namespace() + DwcTerm.basisOfRecord.name())
                        == null
                    && er.getCoreTerms()
                            .get(
                                DwcTerm.occurrenceStatus.namespace() + DwcTerm.basisOfRecord.name())
                        == null;
    AvroCheckPipeline.assertExtendedCountRecords(testOptions1, 5l, notPopulated);

    // Run the interpretation pipeline
    InterpretationPipelineOptions interpretationOptions =
        PipelinesOptionsFactory.create(
            InterpretationPipelineOptions.class,
            new String[] {
              "--datasetId=dr893",
              "--attempt=1",
              "--runner=DirectRunner",
              "--interpretationTypes=ALL",
              "--metaFileName=interpretation-metrics.yml",
              "--targetPath=/tmp/la-pipelines-test/default-values",
              "--inputPath=/tmp/la-pipelines-test/default-values/dr893/1/verbatim.avro",
              "--properties=" + TestUtils.getPipelinesConfigFile(),
              "--useExtendedRecordId=true"
            });
    ALAVerbatimToInterpretedPipeline.run(interpretationOptions);

    // check the interpreted values are NOW populated with default values
    InterpretationPipelineOptions checkPopulatedOptions =
        PipelinesOptionsFactory.create(
            InterpretationPipelineOptions.class,
            new String[] {
              "--datasetId=dr893",
              "--attempt=1",
              "--runner=DirectRunner",
              "--targetPath=/tmp/la-pipelines-test/default-values",
              "--inputPath=/tmp/la-pipelines-test/default-values/dr893/1/interpreted/verbatim/interpret-*",
              "--properties=" + TestUtils.getPipelinesConfigFile()
            });

    // check default values are populated
    Function<ExtendedRecord, Boolean> checkPopulatedFcn =
        (Function<ExtendedRecord, Boolean> & Serializable)
            er ->
                er.getCoreTerms()
                        .containsKey(
                            DwcTerm.basisOfRecord.namespace() + DwcTerm.basisOfRecord.name())
                    && er.getCoreTerms()
                        .containsKey(
                            DwcTerm.occurrenceStatus.namespace() + DwcTerm.occurrenceStatus.name())
                    && er.getCoreTerms()
                        .get(DwcTerm.basisOfRecord.namespace() + DwcTerm.basisOfRecord.name())
                        .equals("HumanObservation")
                    && er.getCoreTerms()
                        .get(DwcTerm.occurrenceStatus.namespace() + DwcTerm.occurrenceStatus.name())
                        .equals("present");

    AvroCheckPipeline.assertExtendedCountRecords(checkPopulatedOptions, 5l, checkPopulatedFcn);
  }
}
