package au.org.ala.clustering;

import java.util.*;
import org.gbif.pipelines.core.parsers.clustering.OccurrenceFeatures;

public class RepresentativeRecordUtils {

  /**
   * Finds the representative record from the set provided. The representative record is chosen by
   * prioritising records by:
   *
   * <p>1) Coordinates 2) Date 3) First added to system
   *
   * @param cluster
   * @return OccurrenceFeatures considered the representative record.
   */
  public static HashKeyOccurrence findRepresentativeRecord(List<HashKeyOccurrence> cluster) {

    // get highest resolution coordinates
    List<HashKeyOccurrence> highestRanking = rankCoordPrecision(cluster);
    if (highestRanking.size() == 1) {
      return highestRanking.get(0);
    }

    // get highest resolution dates
    highestRanking = rankDatePrecision(highestRanking);
    if (highestRanking.size() == 1) {
      return highestRanking.get(0);
    }
    if (highestRanking.size() == 1) {
      return highestRanking.get(0);
    }

    // otherwise, just pick one using a consistent method
    return pickRepresentative(highestRanking);
  }

  public static HashKeyOccurrence pickRepresentative(List<HashKeyOccurrence> cluster) {
    cluster.sort(
        new Comparator<HashKeyOccurrence>() {
          @Override
          public int compare(HashKeyOccurrence o1, HashKeyOccurrence o2) {
            return o1.getId().compareTo(o2.getId());
          }
        });
    return cluster.get(0);
  }

  public static List<HashKeyOccurrence> rankDatePrecision(List<HashKeyOccurrence> cluster) {

    Iterator<HashKeyOccurrence> iter = cluster.iterator();
    HashKeyOccurrence occurrenceFeatures = iter.next();
    Integer highestPrecision = determineDatePrecision(occurrenceFeatures);
    List<HashKeyOccurrence> highestRanking = new ArrayList<>();
    highestRanking.add(occurrenceFeatures);

    while (iter.hasNext()) {

      HashKeyOccurrence occurrenceFeatures1 = iter.next();
      Integer precision = determineDatePrecision(occurrenceFeatures1);
      if (precision == highestPrecision) {
        highestRanking.add(occurrenceFeatures1);
      } else if (precision > highestPrecision) {
        highestRanking.clear();
        highestRanking.add(occurrenceFeatures1);
        highestPrecision = precision;
      }
    }

    // return the highest
    return highestRanking;
  }

  /**
   * Returns a subset of the cluster containing the occurrence features with the highest record
   * precision.
   *
   * @param cluster
   * @return
   */
  public static List<HashKeyOccurrence> rankCoordPrecision(List<HashKeyOccurrence> cluster) {

    Iterator<HashKeyOccurrence> iter = cluster.iterator();
    HashKeyOccurrence occurrenceFeatures = iter.next();
    Integer highestPrecision = determineCoordPrecision(occurrenceFeatures);
    List<HashKeyOccurrence> highestRanking = new ArrayList<>();
    highestRanking.add(occurrenceFeatures);

    while (iter.hasNext()) {

      HashKeyOccurrence occurrenceFeatures1 = iter.next();
      Integer precision = determineCoordPrecision(occurrenceFeatures1);
      if (precision == highestPrecision) {
        highestRanking.add(occurrenceFeatures1);
      } else if (precision > highestPrecision) {
        highestRanking = new ArrayList<>();
        highestRanking.add(occurrenceFeatures1);
        highestPrecision = precision;
      }
    }

    // return the highest
    return highestRanking;
  }

  /**
   * Reports the maximum number of decimal places that the lat/long are reported to Very coarse
   * means to determine precision (logic copied from biocache-store) TODO look for datum,
   * coordinateUncertaintyInMeters
   */
  public static Integer determineCoordPrecision(OccurrenceFeatures occurrenceFeatures) {

    if (occurrenceFeatures.getDecimalLatitude() == null
        || occurrenceFeatures.getDecimalLongitude() == null) {
      return 0;
    }
    Integer latp = extractNoDecPlaces(occurrenceFeatures.getDecimalLatitude());
    Integer lonp = extractNoDecPlaces(occurrenceFeatures.getDecimalLongitude());
    if (latp > lonp) {
      return latp;
    } else {
      return lonp;
    }
  }

  public static Integer extractNoDecPlaces(Double doubleValue) {
    String decimalPlaces = doubleValue.toString().split("\\.")[1];
    if (decimalPlaces.length() > 1) {
      return decimalPlaces.length();
    }
    if (decimalPlaces.length() == 1 && "0".equals(decimalPlaces)) {
      return 0;
    } else {
      return 1;
    }
  }

  /**
   * Reports precision of the date information as a number. A record with year, month and day = 3,
   * with a point removed for each component that is missing.
   */
  public static Integer determineDatePrecision(OccurrenceFeatures occurrenceFeatures) {

    Integer datePrecision = 0;
    if (occurrenceFeatures.getYear() != null) {
      datePrecision += 1;
    }

    if (occurrenceFeatures.getMonth() != null) {
      datePrecision += 1;
    }

    if (occurrenceFeatures.getDay() != null) {
      datePrecision += 1;
    }

    return datePrecision;
  }

  public static List<List<HashKeyOccurrence>> createClusters(List<ClusterPair> pairs) {

    List<List<HashKeyOccurrence>> clusters = new ArrayList<>();

    for (ClusterPair pair : pairs) {
      boolean added = false;

      // iterate through each cluster
      for (List<HashKeyOccurrence> cluster : clusters) {
        boolean present = cluster.contains(pair.getO1()) || cluster.contains(pair.getO2());
        if (present) {
          added = true;
          cluster.add(pair.getO1());
          cluster.add(pair.getO2());
        }
      }

      if (!added) {
        // create a cluster
        List<HashKeyOccurrence> newCluster = new ArrayList<>();
        newCluster.add(pair.getO1());
        newCluster.add(pair.getO2());
        clusters.add(newCluster);
      }
    }
    return clusters;
  }
}