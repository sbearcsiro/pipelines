package org.gbif.pipeleins.config;

import com.google.common.base.Objects;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

/**
 * Configs needed to connect to the occurrence HBase db.
 */
public class OccHBaseConfiguration {

  @Min(1)
  public int hbasePoolSize = 5;

  @NotNull
  public String occTable;

  @NotNull
  public String counterTable;

  @NotNull
  public String lookupTable;

  /**
   * The zookeeper connection being used to create a lock provider
   */
  @NotNull
  public String zkConnectionString;

  /**
   * Uses conventions to populate all table names based on the environment prefix. Only used in tests!
   * @param prefix environment prefix, e.g. prod or uat
   */
  public void setEnvironment(String prefix) {
    occTable = prefix + "_occurrence";
    counterTable = prefix + "_occurrence_counter";
    lookupTable = prefix + "_occurrence_lookup";
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("hbasePoolSize", hbasePoolSize)
      .add("occTable", occTable)
      .add("counterTable", counterTable)
      .add("lookupTable", lookupTable)
      .add("zkConnectionString", zkConnectionString)
      .toString();
  }

}
