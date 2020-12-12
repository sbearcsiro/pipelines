package au.org.ala.pipelines.transforms;

import static org.apache.avro.Schema.Type.UNION;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.AVRO_TO_JSON_COUNT;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.util.Strings;
import org.apache.solr.common.SolrInputDocument;
import org.gbif.pipelines.core.converters.MultimediaConverter;
import org.gbif.pipelines.io.avro.*;
import org.jetbrains.annotations.NotNull;

/**
 * A SOLR transform that aims to provide a index that is backwards compatible with ALA's
 * biocache-service.
 */
@Slf4j
public class IndexRecordTransform implements Serializable {

  private static final long serialVersionUID = 1279313931024806169L;
  // Core
  @NonNull private TupleTag<ExtendedRecord> erTag;
  @NonNull private TupleTag<BasicRecord> brTag;
  @NonNull private TupleTag<TemporalRecord> trTag;
  @NonNull private TupleTag<LocationRecord> lrTag;

  private TupleTag<TaxonRecord> txrTag;
  @NonNull private TupleTag<ALATaxonRecord> atxrTag;
  // Extension
  @NonNull private TupleTag<MultimediaRecord> mrTag;
  @NonNull private TupleTag<ImageRecord> irTag;
  @NonNull private TupleTag<AudubonRecord> arTag;
  @NonNull private TupleTag<MeasurementOrFactRecord> mfrTag;

  private TupleTag<ALAAttributionRecord> aarTag;
  @NonNull private TupleTag<ALAUUIDRecord> urTag;

  @NonNull private TupleTag<ImageServiceRecord> isTag;

  @NonNull private TupleTag<TaxonProfile> tpTag;

  @NonNull private PCollectionView<MetadataRecord> metadataView;

  String datasetID;

  public static IndexRecordTransform create(
      TupleTag<ExtendedRecord> erTag,
      TupleTag<BasicRecord> brTag,
      TupleTag<TemporalRecord> trTag,
      TupleTag<LocationRecord> lrTag,
      TupleTag<TaxonRecord> txrTag,
      TupleTag<ALATaxonRecord> atxrTag,
      TupleTag<MultimediaRecord> mrTag,
      TupleTag<ImageRecord> irTag,
      TupleTag<AudubonRecord> arTag,
      TupleTag<MeasurementOrFactRecord> mfrTag,
      TupleTag<ALAAttributionRecord> aarTag,
      TupleTag<ALAUUIDRecord> urTag,
      TupleTag<ImageServiceRecord> isTag,
      TupleTag<TaxonProfile> tpTag,
      PCollectionView<MetadataRecord> metadataView,
      String datasetID) {
    IndexRecordTransform t = new IndexRecordTransform();
    t.erTag = erTag;
    t.brTag = brTag;
    t.trTag = trTag;
    t.lrTag = lrTag;
    t.txrTag = txrTag;
    t.atxrTag = atxrTag;
    t.mrTag = mrTag;
    t.irTag = irTag;
    t.arTag = arTag;
    t.mfrTag = mfrTag;
    t.aarTag = aarTag;
    t.urTag = urTag;
    t.isTag = isTag;
    t.tpTag = tpTag;
    t.metadataView = metadataView;
    t.datasetID = datasetID;
    return t;
  }

  /**
   * Create a IndexRecord using the supplied records.
   *
   * @param mdr
   * @param er
   * @param br
   * @param tr
   * @param lr
   * @param txr
   * @param atxr
   * @param aar
   * @return
   */
  @NotNull
  public static IndexRecord createIndexRecord(
      MetadataRecord mdr,
      BasicRecord br,
      TemporalRecord tr,
      LocationRecord lr,
      TaxonRecord txr,
      ALATaxonRecord atxr,
      ExtendedRecord er,
      ALAAttributionRecord aar,
      ALAUUIDRecord ur,
      ImageServiceRecord isr,
      TaxonProfile tpr) {

    Set<String> skipKeys = new HashSet<String>();
    skipKeys.add("id");
    skipKeys.add("created");
    skipKeys.add("text");
    skipKeys.add("name");
    skipKeys.add("coreRowType");
    skipKeys.add("coreTerms");
    skipKeys.add("extensions");
    skipKeys.add("usage");
    skipKeys.add("classification");
    skipKeys.add("eventDate");
    skipKeys.add("hasCoordinate");
    skipKeys.add("hasGeospatialIssue");
    skipKeys.add("gbifId");
    skipKeys.add("crawlId");
    skipKeys.add("networkKeys");
    skipKeys.add("protocol");
    skipKeys.add("issues");
    skipKeys.add("machineTags"); // TODO review content

    IndexRecord.Builder indexRecord = IndexRecord.newBuilder().setId(ur.getUuid());
    indexRecord.setBooleans(new HashMap<>());
    indexRecord.setStrings(new HashMap<>());
    indexRecord.setLongs(new HashMap<>());
    indexRecord.setInts(new HashMap<>());
    indexRecord.setDates(new HashMap<>());
    indexRecord.setDoubles(new HashMap<>());
    indexRecord.setMultiValues(new HashMap<>());
    List<String> assertions = new ArrayList<String>();

    addToIndexRecord(lr, indexRecord, skipKeys);
    addToIndexRecord(tr, indexRecord, skipKeys);
    addToIndexRecord(br, indexRecord, skipKeys);
    addToIndexRecord(er, indexRecord, skipKeys);
    addToIndexRecord(mdr, indexRecord, skipKeys);
    addToIndexRecord(mdr, indexRecord, skipKeys);

    // add event date
    try {
      if (tr.getEventDate() != null
          && tr.getEventDate().getGte() != null
          && tr.getEventDate().getGte().length() == 10) {
        indexRecord
            .getDates()
            .put(
                "eventDateSingle",
                new SimpleDateFormat("yyyy-MM-dd").parse(tr.getEventDate().getGte()).getTime());
      }
    } catch (ParseException e) {
      log.error(
          "Unparsable date produced by downstream interpretation " + tr.getEventDate().getGte());
    }

    // GBIF taxonomy - add if available
    if (txr != null) {
      // add the classification
      List<RankedName> taxonomy = txr.getClassification();
      for (RankedName entry : taxonomy) {
        indexRecord
            .getInts()
            .put("gbif_s_" + entry.getRank().toString().toLowerCase() + "_id", entry.getKey());
        indexRecord
            .getStrings()
            .put("gbif_s_" + entry.getRank().toString().toLowerCase(), entry.getName());
      }

      String rank = txr.getAcceptedUsage().getRank().toString();
      indexRecord.getStrings().put("gbif_s_rank", txr.getAcceptedUsage().getRank().toString());
      indexRecord.getStrings().put("gbif_s_scientificName", txr.getAcceptedUsage().getName());
    }

    // Verbatim (Raw) data
    Map<String, String> raw = er.getCoreTerms();
    for (Map.Entry<String, String> entry : raw.entrySet()) {
      String key = entry.getKey();
      if (key.startsWith("http")) {
        key = key.substring(key.lastIndexOf("/") + 1);
      }
      indexRecord.getStrings().put("raw_" + key, entry.getValue());
    }

    if (lr.getHasCoordinate()) {
      addGeo(indexRecord, lr.getDecimalLatitude(), lr.getDecimalLongitude());
    }

    // ALA taxonomy & species groups - backwards compatible for EYA
    if (atxr.getTaxonConceptID() != null) {
      List<Schema.Field> fields = atxr.getSchema().getFields();
      for (Schema.Field field : fields) {
        Object value = atxr.get(field.name());
        if (value != null
            && !field.name().equals("speciesGroup")
            && !field.name().equals("speciesSubgroup")
            && !skipKeys.contains(field.name())) {
          if (field.name().equalsIgnoreCase("issues")) {
            assertions.add((String) value);
          } else {
            if (value instanceof Integer) {
              indexRecord.getInts().put(field.name(), (Integer) value);
            } else {
              indexRecord.getStrings().put(field.name(), value.toString());
            }
          }
        }
      }

      // required for EYA
      indexRecord
          .getStrings()
          .put(
              "names_and_lsid",
              String.join(
                  "|",
                  atxr.getScientificName(),
                  atxr.getTaxonConceptID(),
                  atxr.getVernacularName(),
                  atxr.getKingdom(),
                  atxr.getFamily())); // is set to IGNORE in headerAttributes

      indexRecord
          .getStrings()
          .put(
              "common_name_and_lsid",
              String.join(
                  "|",
                  atxr.getVernacularName(),
                  atxr.getScientificName(),
                  atxr.getTaxonConceptID(),
                  atxr.getVernacularName(),
                  atxr.getKingdom(),
                  atxr.getFamily())); // is set to IGNORE in headerAttribute

      // legacy fields referenced in biocache-service code
      indexRecord.getStrings().put("taxon_name", atxr.getScientificName());
      indexRecord.getStrings().put("lsid", atxr.getTaxonConceptID());
      indexRecord.setTaxonID(atxr.getTaxonConceptID());
      indexRecord.getStrings().put("rank", atxr.getRank());
      indexRecord.getInts().put("rank_id", atxr.getRankID());

      if (atxr.getVernacularName() != null) {
        indexRecord.getStrings().put("common_name", atxr.getVernacularName());
      }

      for (String s : atxr.getSpeciesGroup()) {
        indexRecord.getStrings().put("species_group", s);
      }
      for (String s : atxr.getSpeciesSubgroup()) {
        indexRecord.getStrings().put("species_subgroup", s);
      }
    }

    indexRecord.getBooleans().put("geospatial_kosher", lr.getHasCoordinate());

    // FIXME  - see #162
    if (ur.getFirstLoaded() != null) {
      indexRecord
          .getDates()
          .put(
              "first_loaded_date",
              LocalDateTime.parse(ur.getFirstLoaded(), DateTimeFormatter.ISO_DATE_TIME)
                  .toEpochSecond(ZoneOffset.UTC));
    }

    // Add legacy collectory fields
    if (aar != null) {
      addIfNotEmpty(indexRecord, "license", aar.getLicenseType());
      addIfNotEmpty(
          indexRecord,
          "raw_dataResourceUid",
          aar.getDataResourceUid()); // for backwards compatibility
      addIfNotEmpty(indexRecord, "dataResourceUid", aar.getDataResourceUid());
      addIfNotEmpty(indexRecord, "dataResourceName", aar.getDataResourceName());
      addIfNotEmpty(indexRecord, "dataProviderUid", aar.getDataProviderUid());
      addIfNotEmpty(indexRecord, "dataProviderName", aar.getDataProviderName());
      addIfNotEmpty(indexRecord, "institutionUid", aar.getInstitutionUid());
      addIfNotEmpty(indexRecord, "collectionUid", aar.getCollectionUid());
      addIfNotEmpty(indexRecord, "institutionName", aar.getInstitutionName());
      addIfNotEmpty(indexRecord, "collectionName", aar.getCollectionName());
    }

    // legacy fields reference directly in biocache-service code
    if (txr != null) {
      IssueRecord taxonomicIssues = txr.getIssues();
      assertions.addAll(taxonomicIssues.getIssueList());
    }

    if (isr != null && isr.getImageIDs() != null && !isr.getImageIDs().isEmpty()) {
      indexRecord.getStrings().put("image_url", isr.getImageIDs().get(0));
      indexRecord.getMultiValues().put("all_image_url", isr.getImageIDs());
      // FIX ME - do we need mime type.....
      indexRecord.getStrings().put("multimedia", "Image");
    }

    if (tpr != null && tpr.getSpeciesListID() != null && !tpr.getSpeciesListID().isEmpty()) {

      indexRecord.getMultiValues().put("species_list_uid", tpr.getSpeciesListID());

      // CONSERVATION STATUS
      String stateProvince = lr.getStateProvince();
      String country = lr.getCountry();

      // index conservation status
      List<ConservationStatus> conservationStatuses = tpr.getConservationStatuses();
      for (ConservationStatus conservationStatus : conservationStatuses) {
        if (conservationStatus.getRegion() != null) {
          if (conservationStatus.getRegion().equalsIgnoreCase(stateProvince)) {

            if (Strings.isNotBlank(conservationStatus.getSourceStatus())) {
              indexRecord
                  .getStrings()
                  .put("raw_state_conservation", conservationStatus.getSourceStatus());
            }
            if (Strings.isNotBlank(conservationStatus.getStatus())) {
              indexRecord.getStrings().put("state_conservation", conservationStatus.getStatus());
            }
          }
          if (conservationStatus.getRegion().equalsIgnoreCase(country)) {
            if (Strings.isNotBlank(conservationStatus.getStatus())) {
              indexRecord.getStrings().put("country_conservation", conservationStatus.getStatus());
            }
          }
        }
      }

      // index invasive status
      List<InvasiveStatus> invasiveStatuses = tpr.getInvasiveStatuses();
      for (InvasiveStatus invasiveStatus : invasiveStatuses) {
        if (invasiveStatus.getRegion() != null) {
          if (invasiveStatus.getRegion().equalsIgnoreCase(stateProvince)) {
            indexRecord.getStrings().put("state_invasive", "invasive");
          }
          if (invasiveStatus.getRegion().equalsIgnoreCase(country)) {
            indexRecord.getStrings().put("country_invasive", "invasive");
          }
        }
      }
    }

    assertions.addAll(lr.getIssues().getIssueList());
    assertions.addAll(tr.getIssues().getIssueList());
    assertions.addAll(br.getIssues().getIssueList());
    assertions.addAll(mdr.getIssues().getIssueList());

    indexRecord.getMultiValues().put("assertions", assertions);

    return indexRecord.build();
  }

  public ParDo.SingleOutput<KV<String, CoGbkResult>, IndexRecord> converter() {

    DoFn<KV<String, CoGbkResult>, IndexRecord> fn =
        new DoFn<KV<String, CoGbkResult>, IndexRecord>() {

          private final Counter counter =
              Metrics.counter(IndexRecordTransform.class, AVRO_TO_JSON_COUNT);

          @ProcessElement
          public void processElement(ProcessContext c) {
            CoGbkResult v = c.element().getValue();
            String k = c.element().getKey();

            // Core
            MetadataRecord mdr = c.sideInput(metadataView);
            ExtendedRecord er = v.getOnly(erTag, ExtendedRecord.newBuilder().setId(k).build());
            BasicRecord br = v.getOnly(brTag, BasicRecord.newBuilder().setId(k).build());
            TemporalRecord tr = v.getOnly(trTag, TemporalRecord.newBuilder().setId(k).build());
            LocationRecord lr = v.getOnly(lrTag, LocationRecord.newBuilder().setId(k).build());
            TaxonRecord txr = null;
            if (txrTag != null) {
              txr = v.getOnly(txrTag, TaxonRecord.newBuilder().setId(k).build());
            }

            // Extension
            MultimediaRecord mr = v.getOnly(mrTag, MultimediaRecord.newBuilder().setId(k).build());
            ImageRecord ir = v.getOnly(irTag, ImageRecord.newBuilder().setId(k).build());
            AudubonRecord ar = v.getOnly(arTag, AudubonRecord.newBuilder().setId(k).build());
            MeasurementOrFactRecord mfr =
                v.getOnly(mfrTag, MeasurementOrFactRecord.newBuilder().setId(k).build());

            // ALA specific
            ALAUUIDRecord ur = v.getOnly(urTag);
            ALATaxonRecord atxr = v.getOnly(atxrTag, ALATaxonRecord.newBuilder().setId(k).build());
            ALAAttributionRecord aar =
                v.getOnly(aarTag, ALAAttributionRecord.newBuilder().setId(k).build());

            ImageServiceRecord isr = null;
            if (isTag != null) {
              isr = v.getOnly(isTag, ImageServiceRecord.newBuilder().setId(k).build());
            }

            TaxonProfile tpr = null;
            if (tpTag != null) {
              tpr = v.getOnly(tpTag, TaxonProfile.newBuilder().setId(k).build());
            }

            MultimediaRecord mmr = MultimediaConverter.merge(mr, ir, ar);

            IndexRecord doc = createIndexRecord(mdr, br, tr, lr, txr, atxr, er, aar, ur, isr, tpr);

            c.output(doc);
            counter.inc();
          }
        };

    return ParDo.of(fn).withSideInputs(metadataView);
  }

  static void addIfNotEmpty(IndexRecord.Builder doc, String fieldName, String value) {
    if (StringUtils.isNotEmpty(value)) {
      doc.getStrings().put(fieldName, value);
    }
  }

  static void addGeo(IndexRecord.Builder doc, double lat, double lon) {
    String latlon = "";
    // ensure that the lat longs are in the required range before
    if (lat <= 90 && lat >= -90d && lon <= 180 && lon >= -180d) {
      // https://lucene.apache.org/solr/guide/7_0/spatial-search.html#indexing-points
      latlon = lat + "," + lon; // required format for indexing geodetic points in SOLR
      doc.setLatLng(latlon);
    }

    doc.getStrings().put("lat_long", latlon); // is set to IGNORE in headerAttributes
    doc.getStrings()
        .put("point-1", getLatLongString(lat, lon, "#")); // is set to IGNORE in headerAttributes
    doc.getStrings()
        .put(
            "point-0.1", getLatLongString(lat, lon, "#.#")); // is set to IGNORE in headerAttributes
    doc.getStrings()
        .put(
            "point-0.01",
            getLatLongString(lat, lon, "#.##")); // is set to IGNORE in headerAttributes
    doc.getStrings()
        .put(
            "point-0.02",
            getLatLongStringStep(lat, lon, "#.##", 0.02)); // is set to IGNORE in headerAttributes
    doc.getStrings()
        .put(
            "point-0.001",
            getLatLongString(lat, lon, "#.###")); // is set to IGNORE in headerAttributes
    doc.getStrings()
        .put(
            "point-0.0001",
            getLatLongString(lat, lon, "#.####")); // is set to IGNORE in headerAttributes
  }

  static String getLatLongStringStep(Double lat, Double lon, String format, Double step) {
    DecimalFormat df = new DecimalFormat(format);
    // By some "strange" decision the default rounding model is HALF_EVEN
    df.setRoundingMode(java.math.RoundingMode.HALF_UP);
    return df.format(Math.round(lat / step) * step)
        + ","
        + df.format(Math.round(lon / step) * step);
  }

  /** Returns a lat,long string expression formatted to the supplied Double format */
  static String getLatLongString(Double lat, Double lon, String format) {
    DecimalFormat df = new DecimalFormat(format);
    // By some "strange" decision the default rounding model is HALF_EVEN
    df.setRoundingMode(java.math.RoundingMode.HALF_UP);
    return df.format(lat) + "," + df.format(lon);
  }

  static void addToIndexRecord(
      SpecificRecordBase record, IndexRecord.Builder builder, Set<String> skipKeys) {

    record.getSchema().getFields().stream()
        .filter(n -> !skipKeys.contains(n.name()))
        .forEach(
            f ->
                Optional.ofNullable(record.get(f.pos()))
                    .ifPresent(
                        r -> {
                          Schema schema = f.schema();
                          Optional<Schema.Type> type =
                              schema.getType() == UNION
                                  ? schema.getTypes().stream()
                                      .filter(t -> t.getType() != Schema.Type.NULL)
                                      .findFirst()
                                      .map(Schema::getType)
                                  : Optional.of(schema.getType());
                          if (r != null) {
                            type.ifPresent(
                                t -> {
                                  switch (t) {
                                    case BOOLEAN:
                                      //
                                      builder.getBooleans().put(f.name(), (Boolean) r);
                                      break;
                                    case FLOAT:
                                      builder.getDoubles().put(f.name(), (Double) r);
                                      break;
                                    case DOUBLE:
                                      builder.getDoubles().put(f.name(), (Double) r);
                                      break;
                                    case INT:
                                      builder.getInts().put(f.name(), (Integer) r);
                                      break;
                                    case LONG:
                                      builder.getLongs().put(f.name(), (Long) r);
                                      break;
                                    default:
                                      builder.getStrings().put(f.name(), r.toString());
                                      break;
                                  }
                                });
                          }
                        }));
  }

  /** Transform to create a map of unique keys built from previous runs and UUID. */
  public static class SolrInputDocumentToIndexRecordFcn
      extends DoFn<SolrInputDocument, IndexRecord> {
    @ProcessElement
    public void processElement(
        @Element SolrInputDocument solrInputDocument, OutputReceiver<IndexRecord> out) {
      IndexRecord indexRecord = convertSolrDocToIndexRecord(solrInputDocument);
      out.output(indexRecord);
    }
  }

  public static class IndexRecordToSolrInputDocumentFcn
      extends DoFn<IndexRecord, SolrInputDocument> {
    @ProcessElement
    public void processElement(
        @Element IndexRecord indexRecord, OutputReceiver<SolrInputDocument> out) {
      SolrInputDocument solrInputDocument = convertIndexRecordToSolrDoc(indexRecord);
      out.output(solrInputDocument);
    }
  }

  public static SolrInputDocument convertIndexRecordToSolrDoc(IndexRecord indexRecord) {
    SolrInputDocument doc = new SolrInputDocument();
    doc.setField("id", indexRecord.getId());

    // strings
    for (Map.Entry<String, String> s : indexRecord.getStrings().entrySet()) {
      doc.addField(s.getKey(), s.getValue());
    }

    // doubles
    for (Map.Entry<String, Double> s : indexRecord.getDoubles().entrySet()) {
      doc.addField(s.getKey(), s.getValue());
    }

    // integers
    for (Map.Entry<String, Integer> s : indexRecord.getInts().entrySet()) {
      doc.addField(s.getKey(), s.getValue());
    }

    // multi-value
    for (Map.Entry<String, List<String>> s : indexRecord.getMultiValues().entrySet()) {
      for (String value : s.getValue()) {
        doc.addField(s.getKey(), value);
      }
    }

    return doc;
  }

  public static IndexRecord convertSolrDocToIndexRecord(SolrInputDocument solrInputDocument) {

    IndexRecord.Builder builder =
        IndexRecord.newBuilder().setId(solrInputDocument.getFieldValue("id").toString());

    if (solrInputDocument.getFieldValue("lsid") != null) {
      builder.setTaxonID((String) solrInputDocument.getFieldValue("lsid"));
    }

    if (solrInputDocument.getFieldValue("lat_long") != null) {
      builder.setLatLng((String) solrInputDocument.getFieldValue("lat_long"));
    }

    builder.setStrings(new HashMap<>());
    builder.setInts(new HashMap<>());
    builder.setDoubles(new HashMap<>());
    builder.setMultiValues(new HashMap<>());

    solrInputDocument
        .iterator()
        .forEachRemaining(
            solrInputField -> {
              boolean isMultiValue = solrInputField.getValueCount() > 1;
              String name = solrInputField.getName();
              if (isMultiValue) {
                List<String> values =
                    solrInputField.getValues().stream()
                        .map(value -> value.toString())
                        .filter(value -> Strings.isNotBlank(value))
                        .collect(Collectors.toList());
                builder.getMultiValues().put(name, values);
              } else {
                Object value = solrInputField.getValue();
                if (value != null) {
                  if (value instanceof String) {
                    if (Strings.isNotBlank((String) value)) {
                      builder.getStrings().put(name, (String) value);
                    }
                  } else if (value instanceof Integer) {
                    builder.getInts().put(name, (Integer) value);
                  } else if (value instanceof Double) {
                    builder.getDoubles().put(name, (Double) value);
                  } else if (value instanceof Date) {
                    //              builder.getDateProperties().put(name, (String) value);
                  } else {
                    builder.getStrings().put(name, (String) value.toString());
                  }
                }
              }
            });
    return builder.build();
  }
}
