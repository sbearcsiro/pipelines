package org.gbif.pipelines.transforms.table;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.MEASUREMENT_OR_FACT_TABLE_RECORDS_COUNT;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.gbif.api.vocabulary.Extension;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.core.converters.MeasurementOrFactTableConverter;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.extension.MeasurementOrFactTable;
import org.gbif.pipelines.transforms.core.BasicTransform;
import org.gbif.pipelines.transforms.core.VerbatimTransform;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
@Category(NeedsRunner.class)
public class TableTransformTest {

  @Rule public final transient TestPipeline p = TestPipeline.create();

  @Test
  public void noExtesnsionTest() {

    // State
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("777").build();
    BasicRecord br = BasicRecord.newBuilder().setId("777").setGbifId(777L).build();
    VerbatimTransform verbatimTransform = VerbatimTransform.create();
    BasicTransform basicTransform = BasicTransform.builder().create();

    TableTransform<MeasurementOrFactTable> transform =
        TableTransform.<MeasurementOrFactTable>builder()
            .converterFn(MeasurementOrFactTableConverter::convert)
            .clazz(MeasurementOrFactTable.class)
            .counterName(MEASUREMENT_OR_FACT_TABLE_RECORDS_COUNT)
            .extendedRecordTag(verbatimTransform.getTag())
            .basicRecordTag(basicTransform.getTag())
            .build();

    // When
    PCollection<KV<String, ExtendedRecord>> verbatimCollection =
        p.apply("Create er", Create.of(er)).apply("KV er", verbatimTransform.toKv());

    PCollection<KV<String, BasicRecord>> basicCollection =
        p.apply("Create br", Create.of(br)).apply("KV br", basicTransform.toKv());

    PCollection<MeasurementOrFactTable> result =
        KeyedPCollectionTuple
            // Core
            .of(basicTransform.getTag(), basicCollection)
            .and(verbatimTransform.getTag(), verbatimCollection)
            // Apply
            .apply("Grouping objects", CoGroupByKey.create())
            .apply("Merging", transform.converter());

    // Should
    PAssert.that(result).empty();
    p.run();
  }

  @Test
  public void measurementOrFactTest() {

    // State
    Map<String, String> ext1 = new HashMap<>();
    ext1.put(DwcTerm.measurementID.qualifiedName(), "Id1");
    ext1.put(DwcTerm.measurementType.qualifiedName(), "Type1");
    ext1.put(DwcTerm.measurementValue.qualifiedName(), "1.5");
    ext1.put(DwcTerm.measurementAccuracy.qualifiedName(), "Accurancy1");
    ext1.put(DwcTerm.measurementUnit.qualifiedName(), "Unit1");
    ext1.put(DwcTerm.measurementDeterminedBy.qualifiedName(), "By1");
    ext1.put(DwcTerm.measurementMethod.qualifiedName(), "Method1");
    ext1.put(DwcTerm.measurementRemarks.qualifiedName(), "Remarks1");
    ext1.put(DwcTerm.measurementDeterminedDate.qualifiedName(), "2010/2011");

    Map<String, List<Map<String, String>>> ext = new HashMap<>();
    ext.put(Extension.MEASUREMENT_OR_FACT.getRowType(), Collections.singletonList(ext1));

    ExtendedRecord er = ExtendedRecord.newBuilder().setId("777").setExtensions(ext).build();
    BasicRecord br = BasicRecord.newBuilder().setId("777").setGbifId(777L).build();

    VerbatimTransform verbatimTransform = VerbatimTransform.create();
    BasicTransform basicTransform = BasicTransform.builder().create();
    TableTransform<MeasurementOrFactTable> transform =
        TableTransform.<MeasurementOrFactTable>builder()
            .converterFn(MeasurementOrFactTableConverter::convert)
            .clazz(MeasurementOrFactTable.class)
            .counterName(MEASUREMENT_OR_FACT_TABLE_RECORDS_COUNT)
            .extendedRecordTag(verbatimTransform.getTag())
            .basicRecordTag(basicTransform.getTag())
            .build();

    // When
    PCollection<KV<String, ExtendedRecord>> verbatimCollection =
        p.apply("Create er", Create.of(er)).apply("KV er", verbatimTransform.toKv());

    PCollection<KV<String, BasicRecord>> basicCollection =
        p.apply("Create br", Create.of(br)).apply("KV br", basicTransform.toKv());

    PCollection<MeasurementOrFactTable> result =
        KeyedPCollectionTuple
            // Core
            .of(basicTransform.getTag(), basicCollection)
            .and(verbatimTransform.getTag(), verbatimCollection)
            // Apply
            .apply("Grouping objects", CoGroupByKey.create())
            .apply("Merging", transform.converter());

    // Should
    MeasurementOrFactTable expected =
        MeasurementOrFactTable.newBuilder()
            .setGbifid(777L)
            .setVMeasurementid("Id1")
            .setVMeasurementtype("Type1")
            .setVMeasurementvalue("1.5")
            .setVMeasurementaccuracy("Accurancy1")
            .setVMeasurementunit("Unit1")
            .setVMeasurementdeterminedby("By1")
            .setVMeasurementmethod("Method1")
            .setVMeasurementremarks("Remarks1")
            .setVMeasurementdetermineddate("2010/2011")
            .setMeasurementid("Id1")
            .setMeasurementtype("Type1")
            .setMeasurementvalue("1.5")
            .setMeasurementaccuracy("Accurancy1")
            .setMeasurementunit("Unit1")
            .setMeasurementdeterminedby("By1")
            .setMeasurementmethod("Method1")
            .setMeasurementremarks("Remarks1")
            .setMeasurementdetermineddate("2010/2011")
            .build();

    PAssert.that(result).containsInAnyOrder(expected);
    p.run();
  }
}
