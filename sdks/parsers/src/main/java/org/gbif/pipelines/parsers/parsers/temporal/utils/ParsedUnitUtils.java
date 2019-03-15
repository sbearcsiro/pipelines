package org.gbif.pipelines.parsers.parsers.temporal.utils;

import java.text.DateFormatSymbols;
import java.time.Year;
import java.util.Optional;
import java.util.function.Predicate;

import static org.apache.commons.lang3.StringUtils.isNumeric;

import static com.google.common.base.Strings.isNullOrEmpty;

/** Util class for parsing values */
public class ParsedUnitUtils {

  // Cached instance
  private static final String[] MONTHS = DateFormatSymbols.getInstance().getMonths();

  private ParsedUnitUtils() {
    // Can't have an instance
  }

  public static Optional<Integer> parseYear(String year) {
    return parseInteger(year, x -> x > Year.now().getValue() || x < 1000);
  }

  public static Optional<Integer> parseMonth(String month) {
    if (isNullOrEmpty(month)) {
      return Optional.empty();
    }
    return isNumeric(month) ? parseMonthAsInt(month) : parseMonthAsString(month);
  }

  public static Optional<Integer> parseDay(String day) {
    return parseInteger(day, x -> x < 1 || x > 31);
  }

  public static Optional<Integer> parseHour(String hour) {
    return parseInteger(hour, x -> x < 0 || x > 23);
  }

  public static Optional<Integer> parseMinute(String minute) {
    return parseInteger(minute, x -> x < 0 || x > 59);
  }

  public static Optional<Integer> parseSecond(String second) {
    return parseInteger(second, x -> x < 0 || x > 59);
  }

  private static Optional<Integer> parseMonthAsString(String month) {
    for (int x = 0; x < MONTHS.length; x++) {
      if (MONTHS[x].toLowerCase().startsWith(month.toLowerCase())) {
        return Optional.of(x + 1);
      }
    }
    return Optional.empty();
  }

  private static Optional<Integer> parseMonthAsInt(String month) {
    return parseInteger(month, x -> x < 1 || x > 12);
  }

  /**
   * Common method for parsing short numeric string to int
   *
   * @param rawValue raw value for parsing
   * @param validator predicate with validity conditions
   * @return parsed value or ISSUE(-1) value, if value is invalid
   */
  private static Optional<Integer> parseInteger(String rawValue, Predicate<Integer> validator) {
    Integer value =
        (!isNullOrEmpty(rawValue) && isNumeric(rawValue) && rawValue.length() < 5)
            ? Integer.valueOf(rawValue)
            : -1;
    return validator.test(value) ? Optional.empty() : Optional.of(value);
  }
}
