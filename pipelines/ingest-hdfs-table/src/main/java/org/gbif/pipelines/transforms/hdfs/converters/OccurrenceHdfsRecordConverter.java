package org.gbif.pipelines.transforms.hdfs.converters;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Year;
import java.time.YearMonth;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.dwc.terms.TermFactory;
import org.gbif.occurrence.download.hive.HiveColumns;
import org.gbif.occurrence.download.hive.Terms;
import org.gbif.pipelines.core.utils.TemporalUtils;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.IssueRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.io.avro.Multimedia;
import org.gbif.pipelines.io.avro.MultimediaRecord;
import org.gbif.pipelines.io.avro.OccurrenceHdfsRecord;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.gbif.pipelines.transforms.hdfs.utils.MediaSerDeserUtils;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.commons.beanutils.PropertyUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.base.Strings;

/**
 * Utility class to convert interpreted and extended records into {@link OccurrenceHdfsRecord}.
 */
public class OccurrenceHdfsRecordConverter {

  //Registered converters
  private static Map<Class<? extends SpecificRecordBase>, BiConsumer<OccurrenceHdfsRecord,SpecificRecordBase>>
    converters;

  //Converters
  static {
    converters = new HashMap<>();
    converters.put(ExtendedRecord.class, extendedRecordMapper());
    converters.put(BasicRecord.class, basicRecordMapper());
    converters.put(LocationRecord.class, locationMapper());
    converters.put(TaxonRecord.class, taxonMapper());
    converters.put(TemporalRecord.class, temporalMapper());
    converters.put(MetadataRecord.class, metadataMapper());
    converters.put(MultimediaRecord.class, multimediaMapper());
  }

  //Converts a TemporalAccessor into Date
  private static final Function<TemporalAccessor, Date> TEMPORAL_TO_DATE =
    temporalAccessor -> {
      if (temporalAccessor instanceof ZonedDateTime) {
        return Date.from(((ZonedDateTime)temporalAccessor).toInstant());
      } else if (temporalAccessor instanceof LocalDateTime) {
        return Date.from(((LocalDateTime)temporalAccessor).toInstant(ZoneOffset.UTC));
      } else if (temporalAccessor instanceof LocalDate) {
        return Date.from((((LocalDate)temporalAccessor).atStartOfDay()).toInstant(ZoneOffset.UTC));
      } else if (temporalAccessor instanceof YearMonth) {
        return Date.from((((YearMonth)temporalAccessor).atDay(1)).atStartOfDay().toInstant(ZoneOffset.UTC));
      } else if (temporalAccessor instanceof Year) {
        return Date.from((((Year)temporalAccessor).atDay(1)).atStartOfDay().toInstant(ZoneOffset.UTC));
      } else {
        return null;
      }
    };

  //Supported Date formats
  private static final DateTimeFormatter FORMATTER =
    DateTimeFormatter.ofPattern(
      "[yyyy-MM-dd'T'HH:mm:ss.SSS XXX][yyyy-MM-dd'T'HH:mm:ss.SSSXXX][yyyy-MM-dd'T'HH:mm:ss.SSS]"
      + "[yyyy-MM-dd'T'HH:mm:ss][yyyy-MM-dd'T'HH:mm:ss XXX][yyyy-MM-dd'T'HH:mm:ssXXX][yyyy-MM-dd'T'HH:mm:ss]"
      + "[yyyy-MM-dd'T'HH:mm][yyyy-MM-dd][yyyy-MM][yyyy]")
      .withZone(ZoneId.of("UTC"));

  //Converts a String into Date
  private static final Function<String, Date> STRING_TO_DATE =
    dateAsString -> {
      if (Strings.isNullOrEmpty(dateAsString)) {
        return null;
      }

      // parse string
      TemporalAccessor temporalAccessor = FORMATTER.parseBest(dateAsString,
                                                              ZonedDateTime::from,
                                                              LocalDateTime::from,
                                                              LocalDate::from,
                                                              YearMonth::from,
                                                              Year::from);
      return TEMPORAL_TO_DATE.apply(temporalAccessor);
    };

  private static final TermFactory TERM_FACTORY =  TermFactory.instance();


  private static final Logger LOG = LoggerFactory.getLogger(OccurrenceHdfsRecordConverter.class);

  /**
   * Adds the list of issues to the list of issues in the {@link OccurrenceHdfsRecord}.
   * @param issueRecord
   * @param hr target object
   */
  private static void addIssues(IssueRecord issueRecord, OccurrenceHdfsRecord hr) {
    if (Objects.nonNull(issueRecord) && Objects.nonNull(issueRecord.getIssueList())) {
      List<String> currentIssues =  hr.getIssue();
      currentIssues.addAll(issueRecord.getIssueList());
      hr.setIssue(currentIssues);
    }
  }

  /**
   * Copies the {@link LocationRecord} data into the {@link OccurrenceHdfsRecord}.
   */
  private static BiConsumer<OccurrenceHdfsRecord,SpecificRecordBase> locationMapper() {
    return (hr, sr) -> {
      LocationRecord lr = (LocationRecord)sr;
      hr.setCountrycode(lr.getCountryCode());
      hr.setCounty(lr.getCountry());
      hr.setContinent(lr.getContinent());
      hr.setDecimallatitude(lr.getDecimalLatitude());
      hr.setDecimallongitude(lr.getDecimalLongitude());
      hr.setCoordinateprecision(lr.getCoordinatePrecision());
      hr.setCoordinateuncertaintyinmeters(lr.getCoordinateUncertaintyInMeters());
      hr.setDepth(lr.getDepth());
      hr.setDepthaccuracy(lr.getDepthAccuracy());
      hr.setElevation(lr.getElevation());
      hr.setElevationaccuracy(lr.getElevationAccuracy());
      if (Objects.nonNull(lr.getMaximumDistanceAboveSurfaceInMeters())) {
        hr.setMaximumdistanceabovesurfaceinmeters(lr.getMaximumDistanceAboveSurfaceInMeters()
                                                    .toString());
      }
      if (Objects.nonNull(lr.getMinimumDistanceAboveSurfaceInMeters())) {
        hr.setMinimumdistanceabovesurfaceinmeters(lr.getMinimumDistanceAboveSurfaceInMeters()
                                                    .toString());
      }
      hr.setStateprovince(lr.getStateProvince());
      hr.setWaterbody(lr.getWaterBody());
      hr.setHascoordinate(lr.getHasCoordinate());
      hr.setHasgeospatialissues(lr.getHasGeospatialIssue());
      hr.setRepatriated(lr.getRepatriated());
      addIssues(lr.getIssues(), hr);
    };
  }

  /**
   * Copies the {@link MetadataRecord} data into the {@link OccurrenceHdfsRecord}.
   */
  private static BiConsumer<OccurrenceHdfsRecord, SpecificRecordBase> metadataMapper() {
    return (hr, sr) -> {
      MetadataRecord mr = (MetadataRecord)sr;
      hr.setCrawlid(mr.getCrawlId());
      hr.setDatasetkey(mr.getDatasetKey());
      hr.setPublishingcountry(mr.getDatasetPublishingCountry());
      hr.setDatasetname(mr.getDatasetTitle());
      hr.setInstallationkey(mr.getInstallationKey());
      hr.setLicense(mr.getLicense());
      hr.setProtocol(mr.getProtocol());
      hr.setNetworkkey(mr.getNetworkKeys());
      hr.setPublisher(mr.getPublisherTitle());
      hr.setPublishingorgkey(mr.getPublishingOrganizationKey());
      hr.setPublishingcountry(mr.getDatasetPublishingCountry());
      hr.setLastcrawled(mr.getLastCrawled());
      addIssues(mr.getIssues(), hr);
    };
  }


  /**
   * Copies the {@link TemporalRecord} data into the {@link OccurrenceHdfsRecord}.
   */
  private static BiConsumer<OccurrenceHdfsRecord, SpecificRecordBase> temporalMapper() {
    return (hr, sr) -> {
      TemporalRecord tr = (TemporalRecord)sr;
      Optional.ofNullable(tr.getDateIdentified()).map(STRING_TO_DATE).ifPresent(date -> hr.setDateidentified(date.getTime()));
      Optional.ofNullable(tr.getModified()).map(STRING_TO_DATE).ifPresent(date -> hr.setModified(date.getTime()));
      hr.setDay(tr.getDay());
      hr.setMonth(tr.getMonth());
      hr.setYear(tr.getYear());

      if (Objects.nonNull(tr.getStartDayOfYear())) {
        hr.setStartdayofyear(tr.getStartDayOfYear().toString());
      }


      if (Objects.nonNull(tr.getEndDayOfYear())) {
        hr.setEnddayofyear(tr.getEndDayOfYear().toString());
      }

      TemporalUtils.getTemporal(tr.getYear(), tr.getMonth(), tr.getDay())
        .map(TEMPORAL_TO_DATE)
        .ifPresent(eventDate -> hr.setEventdate(eventDate.getTime()));
      addIssues(tr.getIssues(), hr);
    };
  }

  /**
   * Copies the {@link TaxonRecord} data into the {@link OccurrenceHdfsRecord}.
   */
  private static BiConsumer<OccurrenceHdfsRecord, SpecificRecordBase> taxonMapper() {
    return (hr, sr) -> {
      TaxonRecord tr = (TaxonRecord) sr;
      Optional.ofNullable(tr.getUsage()).ifPresent(x -> hr.setTaxonkey(x.getKey()));
      if (Objects.nonNull(tr.getClassification())) {
        tr.getClassification().forEach(rankedName -> {
          switch (rankedName.getRank()) {
            case KINGDOM:
              hr.setKingdom(rankedName.getName());
              hr.setKingdomkey(rankedName.getKey());
              break;
            case PHYLUM:
              hr.setPhylum(rankedName.getName());
              hr.setPhylumkey(rankedName.getKey());
              break;
            case CLASS:
              hr.setClass$(rankedName.getName());
              hr.setClasskey(rankedName.getKey());
              break;
            case ORDER:
              hr.setOrder(rankedName.getName());
              hr.setOrderkey(rankedName.getKey());
              break;
            case FAMILY:
              hr.setFamily(rankedName.getName());
              hr.setFamilykey(rankedName.getKey());
              break;
            case GENUS:
              hr.setGenus(rankedName.getName());
              hr.setGenuskey(rankedName.getKey());
              break;
            case SUBGENUS:
              hr.setSubgenus(rankedName.getName());
              hr.setSubgenuskey(rankedName.getKey());
              break;
            case SPECIES:
              hr.setSpecies(rankedName.getName());
              hr.setSpecieskey(rankedName.getKey());
              break;
          }
        });
      }

      if (Objects.nonNull(tr.getAcceptedUsage())) {
        hr.setAcceptedscientificname(tr.getAcceptedUsage().getName());
        hr.setAcceptednameusageid(tr.getAcceptedUsage().getKey().toString());
        if (Objects.nonNull(tr.getAcceptedUsage().getKey())) {
          hr.setAcceptedtaxonkey(tr.getAcceptedUsage().getKey());
        }
      } else if (Objects.nonNull(tr.getUsage())) {
        hr.setAcceptedtaxonkey(tr.getUsage().getKey());
        hr.setAcceptedscientificname(tr.getUsage().getName());
      }

      if (Objects.nonNull(tr.getUsageParsedName())) {
        hr.setGenericname(Objects.nonNull(tr.getUsageParsedName().getGenus())
                            ? tr.getUsageParsedName().getGenus()
                            : tr.getUsageParsedName().getUninomial());
        hr.setSpecificepithet(tr.getUsageParsedName().getSpecificEpithet());
        hr.setInfraspecificepithet(tr.getUsageParsedName().getInfraspecificEpithet());
      }
      addIssues(tr.getIssues(), hr);
    };
  }

  /**
   * Copies the {@link BasicRecord} data into the {@link OccurrenceHdfsRecord}.
   */
  private static BiConsumer<OccurrenceHdfsRecord, SpecificRecordBase> basicRecordMapper() {
    return (hr, sr) -> {
      BasicRecord br = (BasicRecord)sr;
      if (Objects.nonNull(br.getGbifId())) {
        hr.setGbifid(br.getGbifId());
      }
      hr.setBasisofrecord(br.getBasisOfRecord());
      hr.setEstablishmentmeans(br.getEstablishmentMeans());
      hr.setIndividualcount(br.getIndividualCount());
      hr.setLifestage(br.getLifeStage());
      hr.setReferences(br.getReferences());
      hr.setSex(br.getSex());
      hr.setTypestatus(br.getTypeStatus());
      hr.setTypifiedname(br.getTypifiedName());
      if (Objects.nonNull(br.getCreated())) {
        hr.setLastcrawled(br.getCreated());
        hr.setLastinterpreted(br.getCreated());
        hr.setLastinterpreted(br.getCreated());
      }
      hr.setCreated(new Date(br.getCreated()).toString());
      addIssues(br.getIssues(), hr);
    };
  }

  /**
   * From a {@link Schema.Field} copies it value into a the {@link OccurrenceHdfsRecord} field using the recognized data type.
   * @param occurrenceHdfsRecord target record
   * @param avroField field to be copied
   * @param fieldName {@link OccurrenceHdfsRecord} field/property name
   * @param value field data/value
   */
  private static void setHdfsRecordField(OccurrenceHdfsRecord occurrenceHdfsRecord, Schema.Field avroField, String fieldName, String value) {
    try {
      Schema.Type fieldType = avroField.schema().getType();
      if (Schema.Type.UNION == avroField.schema().getType()) {
        fieldType = avroField.schema().getTypes().get(0).getType();
      }
      switch (fieldType) {
        case INT:
          PropertyUtils.setProperty(occurrenceHdfsRecord, fieldName, Integer.valueOf(value));
          break;
        case LONG:
          PropertyUtils.setProperty(occurrenceHdfsRecord, fieldName, Long.valueOf(value));
          break;
        case BOOLEAN:
          PropertyUtils.setProperty(occurrenceHdfsRecord, fieldName, Boolean.valueOf(value));
          break;
        case DOUBLE:
          PropertyUtils.setProperty(occurrenceHdfsRecord, fieldName, Double.valueOf(value));
          break;
        case FLOAT:
          PropertyUtils.setProperty(occurrenceHdfsRecord, fieldName, Float.valueOf(value));
          break;
        default:
          PropertyUtils.setProperty(occurrenceHdfsRecord, fieldName, value);
          break;
      }
    } catch (Exception ex) {
      LOG.error("Ignoring error setting field {}", avroField, ex);
    }
  }


  /**
   * Copies the {@link ExtendedRecord} data into the {@link OccurrenceHdfsRecord}.
   */
  private static BiConsumer<OccurrenceHdfsRecord, SpecificRecordBase> extendedRecordMapper() {
    return (hr, sr) -> {
      ExtendedRecord er = (ExtendedRecord)sr;
      er.getCoreTerms().forEach((k, v) -> Optional.ofNullable(TERM_FACTORY.findTerm(k)).ifPresent(term -> {

        if (Terms.verbatimTerms().contains(term)) {
          Optional.ofNullable(verbatimSchemaField(term)).ifPresent(field -> {
            String verbatimField = "V" + field.name().substring(2, 3).toUpperCase() + field.name().substring(3);
            setHdfsRecordField(hr, field, verbatimField, v);
          });
        }
        Optional.ofNullable(interpretedSchemaField(term)).ifPresent(field -> {
          String interpretedFieldname = field.name();
          if (DcTerm.abstract_ == term) {
            interpretedFieldname = "abstract$";
          } else if (DwcTerm.class_ == term) {
            interpretedFieldname = "class$";
          } else if (DwcTerm.group == term) {
            interpretedFieldname = "group";
          } else if (DwcTerm.order == term) {
            interpretedFieldname = "order";
          } else if (DcTerm.date == term) {
            interpretedFieldname = "date";
          } else if (DcTerm.format == term) {
            interpretedFieldname = "format";
          }
          setHdfsRecordField(hr, field, interpretedFieldname, v);
        });
      }));
    };
  }

  /**
   * Collects data from {@link SpecificRecordBase} instances into a {@link OccurrenceHdfsRecord}.
   * @param records list of input records
   * @return a {@link OccurrenceHdfsRecord} instance based on the input records
   */
  public static OccurrenceHdfsRecord toOccurrenceHdfsRecord(SpecificRecordBase...records) {
    OccurrenceHdfsRecord occurrenceHdfsRecord = new OccurrenceHdfsRecord();
    occurrenceHdfsRecord.setIssue(new ArrayList<>());
    for (SpecificRecordBase record : records) {
      Optional.ofNullable(converters.get(record.getClass()))
        .ifPresent(consumer -> consumer.accept(occurrenceHdfsRecord, record));
    }
    return occurrenceHdfsRecord;
  }

  /**
   * Collects the {@link MultimediaRecord}  mediaTypes data into the {@link OccurrenceHdfsRecord#setMediatype(List)}.
   */
  private static BiConsumer<OccurrenceHdfsRecord, SpecificRecordBase> multimediaMapper() {
    return (hr, sr) -> {
      MultimediaRecord mr = (MultimediaRecord)sr;
      // media types
      List<String> mediaTypes = mr.getMultimediaItems().stream()
        .filter(i -> !Strings.isNullOrEmpty(i.getType()))
        .map(Multimedia::getType)
        .map(TextNode::valueOf)
        .map(TextNode::asText)
        .collect(Collectors.toList());
      hr.setExtMultimedia(MediaSerDeserUtils.toJson(mr.getMultimediaItems()));

      hr.setMediatype(mediaTypes);
    };
  }

  /**
   * Gets the {@link Schema.Field} associated to a verbatim term.
   */
  private static Schema.Field verbatimSchemaField(Term term) {
    return OccurrenceHdfsRecord.SCHEMA$.getField("v_" + HiveColumns.columnFor(term));
  }

  /**
   * Gets the {@link Schema.Field} associated to a interpreted term.
   */
  private static Schema.Field interpretedSchemaField(Term term) {
    return OccurrenceHdfsRecord.SCHEMA$.getField(HiveColumns.columnFor(term));
  }
}
