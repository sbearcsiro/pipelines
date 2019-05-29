package org.gbif.pipelines.parsers.parsers.temporal;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.MonthDay;
import java.time.Year;
import java.time.YearMonth;
import java.time.temporal.Temporal;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.gbif.pipelines.parsers.parsers.temporal.ParsedTemporalIssue.DATE_INVALID;
import static org.gbif.pipelines.parsers.parsers.temporal.ParsedTemporalIssue.DATE_MISMATCH;
import static org.gbif.pipelines.parsers.parsers.temporal.ParsedTemporalIssue.DATE_UNLIKELY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(JUnit4.class)
public class TemporalParserTest {

  @Test
  public void allNullTest() {
    // State
    String eventDate = null;
    String year = null;
    String month = null;
    String day = null;

    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);

    // Should
    assertFalse(result.getFromOpt().isPresent());
    assertFalse(result.getToOpt().isPresent());
    assertFalse(result.getYearOpt().isPresent());
    assertFalse(result.getMonthOpt().isPresent());
    assertFalse(result.getDayOpt().isPresent());
    assertTrue(result.getIssues().isEmpty());
  }

  @Test
  public void yearOnlyTest() {
    // State
    Temporal expectedFirst = Year.of(1999);

    String eventDate = null;
    String year = "1999";
    String month = null;
    String day = null;

    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);
    // Should
    assertEquals(expectedFirst, result.getFromDate());
    assertEquals(expectedFirst, result.getYear());
    assertFalse(result.getToOpt().isPresent());
    assertFalse(result.getMonthOpt().isPresent());
    assertFalse(result.getDayOpt().isPresent());
    assertTrue(result.getIssues().isEmpty());
  }

  @Test
  public void yearAndEventDateNullTest() {
    // State
    String eventDate = null;
    String year = null;
    String month = "04";
    String day = "01";

    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);
    // Should
    assertFalse(result.getFromOpt().isPresent());
    assertFalse(result.getToOpt().isPresent());
    assertFalse(result.getYearOpt().isPresent());
    assertFalse(result.getMonthOpt().isPresent());
    assertFalse(result.getDayOpt().isPresent());
    assertEquals(1, result.getIssues().size());
    assertTrue(result.getIssues().contains(DATE_INVALID));
  }

  @Test
  public void notAdateTest() {
    // State
    String eventDate = "not a date";
    String year = null;
    String month = null;
    String day = null;

    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);
    // Should
    assertFalse(result.getFromOpt().isPresent());
    assertFalse(result.getToOpt().isPresent());
    assertFalse(result.getYearOpt().isPresent());
    assertFalse(result.getMonthOpt().isPresent());
    assertFalse(result.getDayOpt().isPresent());
    assertEquals(1, result.getIssues().size());
    assertTrue(result.getIssues().contains(DATE_INVALID));
  }

  @Test
  public void eventDateNullTest() {
    // State
    Temporal expectedFirst = LocalDate.of(1999, 4, 1);

    String eventDate = null;
    String year = "1999";
    String month = "4";
    String day = "1";

    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);
    // Should
    assertEquals(expectedFirst, result.getFromDate());
    assertFalse(result.getToOpt().isPresent());
    assertEquals(Year.parse(year), result.getYear());
    assertEquals(Month.APRIL, result.getMonth());
    assertEquals(Integer.parseInt(day), result.getDay().intValue());
    assertTrue(result.getIssues().isEmpty());
  }

  @Test
  public void eventDateEmptyTest() {
    // State
    Temporal expectedFirst = LocalDate.of(1999, 4, 1);

    String eventDate = "";
    String year = "1999";
    String month = "04";
    String day = "01";
    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);
    // Should
    assertEquals(expectedFirst, result.getFromDate());
    assertFalse(result.getToOpt().isPresent());
    assertEquals(Year.parse(year), result.getYear());
    assertEquals(Month.APRIL, result.getMonth());
    assertEquals(Integer.parseInt(day), result.getDay().intValue());
    assertTrue(result.getIssues().isEmpty());
  }

  @Test
  public void eventDateYearOnlyTest() {
    // State
    Temporal expectedFirst = LocalDate.of(1999, 4, 1);

    String eventDate = "1999";
    String year = "1999";
    String month = "04";
    String day = "01";
    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);
    // Should
    assertEquals(expectedFirst, result.getFromDate());
    assertFalse(result.getToOpt().isPresent());
    assertEquals(Year.parse(year), result.getYear());
    assertEquals(Month.APRIL, result.getMonth());
    assertEquals(Integer.parseInt(day), result.getDay().intValue());
    assertTrue(result.getIssues().isEmpty());
  }

  @Test
  public void eventDateYearAndDayTest() {
    // State
    Temporal expectedFirst = Year.of(2000);

    String eventDate = "2000";
    String year = "2000";
    String month = null;
    String day = "04";
    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);
    // Should
    assertEquals(expectedFirst, result.getFromDate());
    assertFalse(result.getToOpt().isPresent());
    assertEquals(Year.parse(year), result.getYear());
    assertFalse(result.getMonthOpt().isPresent());
    assertFalse(result.getDayOpt().isPresent());
    assertTrue(result.getIssues().isEmpty());
  }

  @Test
  public void eventDateYearMonthOnlyTest() {
    // State
    Temporal expectedFirst = LocalDate.of(1999, 4, 1);

    String eventDate = "1999-04";
    String year = "1999";
    String month = "04";
    String day = "01";
    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);
    // Should
    assertEquals(expectedFirst, result.getFromDate());
    assertFalse(result.getToOpt().isPresent());
    assertEquals(Year.parse(year), result.getYear());
    assertEquals(Month.APRIL, result.getMonth());
    assertEquals(Integer.parseInt(day), result.getDay().intValue());
    assertTrue(result.getIssues().isEmpty());
  }

  @Test
  public void localDateIsoTest() {
    // State
    Temporal expectedFirst = LocalDate.of(1999, 4, 5);

    String eventDate = "1999-04-05";
    String year = "1999";
    String month = "04";
    String day = "05";
    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);
    // Should
    assertEquals(expectedFirst, result.getFromDate());
    assertFalse(result.getToOpt().isPresent());
    assertEquals(Year.parse(year), result.getYear());
    assertEquals(Month.APRIL, result.getMonth());
    assertEquals(Integer.parseInt(day), result.getDay().intValue());
    assertTrue(result.getIssues().isEmpty());
  }

  @Test
  public void localDateIso2Test() {
    // State
    Temporal expectedFirst = LocalDate.of(1999, 4, 5);

    String eventDate = "1999-04-05";
    String year = "1999";
    String month = null;
    String day = null;
    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);
    // Should
    assertEquals(expectedFirst, result.getFromDate());
    assertEquals(Year.from(expectedFirst), result.getYear());
    assertEquals(Month.from(expectedFirst), result.getMonth());
    assertEquals(MonthDay.from(expectedFirst).getDayOfMonth(), result.getDay().intValue());
    assertFalse(result.getToOpt().isPresent());
  }

  @Test
  public void localDateIso3Test() {
    // State
    Temporal expectedFirst = LocalDate.of(1999, 4, 5);
    Temporal expectedSecond = LocalDate.of(2000, 6, 5);

    String eventDate = "1999-04-05/2000-06-05";
    String year = "2000";
    String month = "05";
    String day = "03";
    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);
    // Should
    assertEquals(expectedFirst, result.getFromDate());
    assertEquals(expectedSecond, result.getToDate());
    assertEquals(Year.parse(year), result.getYear());
    assertEquals(Month.MAY, result.getMonth());
    assertEquals(Integer.parseInt(day), result.getDay().intValue());
    assertEquals(1, result.getIssues().size());
    assertTrue(result.getIssues().contains(DATE_MISMATCH));
  }

  @Test
  public void localDateIso4Test() {
    // State
    Temporal expectedFirst = LocalDate.of(1999, 4, 5);
    Temporal expectedSecond = LocalDate.of(2000, 6, 5);

    String eventDate = "1999-04-05/2000-06-05";
    String year = "2000";
    String month = "05";
    String day = "";
    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);
    // Should
    assertEquals(expectedFirst, result.getFromDate());
    assertEquals(expectedSecond, result.getToDate());
    assertEquals(Year.parse(year), result.getYear());
    assertEquals(Month.MAY, result.getMonth());
    assertFalse(result.getDayOpt().isPresent());
    assertEquals(1, result.getIssues().size());
    assertTrue(result.getIssues().contains(DATE_MISMATCH));
  }

  @Test
  public void localDateTimeIsoTest() {
    // State
    Temporal expectedFirst = LocalDateTime.of(1999, 4, 1, 9, 26, 0);

    String eventDate = "1999-04-01T09:26Z";
    String year = "1999";
    String month = "04";
    String day = "01";
    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);
    // Should
    assertEquals(expectedFirst, result.getFromDate());
    assertFalse(result.getToOpt().isPresent());
    assertEquals(Year.parse(year), result.getYear());
    assertEquals(Month.APRIL, result.getMonth());
    assertEquals(Integer.parseInt(day), result.getDay().intValue());
    assertTrue(result.getIssues().isEmpty());
  }

  @Test
  public void yearPeriodOnlyTest() {
    // State
    Temporal expectedFirst = Year.of(1999);
    Temporal expectedSecond = Year.of(2010);

    String eventDate = "1999/2010";
    String year = "1999";
    String month = "01";
    String day = "01";
    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);
    // Should
    assertEquals(expectedFirst, result.getFromDate());
    assertEquals(expectedSecond, result.getToDate());
    assertEquals(Year.parse(year), result.getYear());
    assertEquals(Month.JANUARY, result.getMonth());
    assertEquals(Integer.parseInt(day), result.getDay().intValue());
    assertEquals(1, result.getIssues().size());
    assertTrue(result.getIssues().contains(DATE_MISMATCH));
  }

  @Test
  public void fullYearMonthPeriodTest() {
    // State
    Temporal expectedFirst = YearMonth.of(1999, 4);
    Temporal expectedSecond = YearMonth.of(2010, 1);

    String eventDate = "1999-04/2010-01";
    String year = "1999";
    String month = "04";
    String day = "01";
    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);
    // Should
    assertEquals(expectedFirst, result.getFromDate());
    assertEquals(expectedSecond, result.getToDate());
    assertEquals(Year.parse(year), result.getYear());
    assertEquals(Month.APRIL, result.getMonth());
    assertEquals(Integer.parseInt(day), result.getDay().intValue());
    assertEquals(1, result.getIssues().size());
    assertTrue(result.getIssues().contains(DATE_MISMATCH));
  }

  @Test
  public void fullLocalDatePeriodTest() {
    // State
    Temporal expectedFirst = LocalDate.of(1999, 4, 11);
    Temporal expectedSecond = LocalDate.of(2009, 10, 8);

    String eventDate = "1999-04-11/2009-10-08";
    String year = "1999";
    String month = "04";
    String day = "12";
    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);
    // Should
    assertEquals(expectedFirst, result.getFromDate());
    assertEquals(expectedSecond, result.getToDate());
    assertEquals(Year.parse(year), result.getYear());
    assertEquals(Month.APRIL, result.getMonth());
    assertEquals(Integer.parseInt(day), result.getDay().intValue());
    assertEquals(1, result.getIssues().size());
    assertTrue(result.getIssues().contains(DATE_MISMATCH));
  }

  @Test
  public void fullLocalDateTimePeriodTest() {
    // State
    Temporal expectedFirst = LocalDateTime.of(1999, 4, 17, 12, 26, 0);
    Temporal expectedSecond = LocalDateTime.of(1999, 4, 17, 12, 52, 17);

    String eventDate = "1999-04-17T12:26Z/12:52:17Z";
    String year = "1999";
    String month = "04";
    String day = "17";
    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);
    // Should
    assertEquals(expectedFirst, result.getFromDate());
    assertEquals(expectedSecond, result.getToDate());
    assertEquals(Year.parse(year), result.getYear());
    assertEquals(Month.APRIL, result.getMonth());
    assertEquals(Integer.parseInt(day), result.getDay().intValue());
    assertTrue(result.getIssues().isEmpty());
  }

  @Test
  public void fullLocalDateTimePeriodSkipZoneTest() {
    // State
    Temporal expectedFirst = LocalDateTime.of(1999, 4, 8, 14, 7, 0);
    Temporal expectedSecond = LocalDateTime.of(2010, 8, 3, 6, 0, 0);

    String eventDate = "1999-04-08T14:07-0600/2010-08-03T06:00-0000";
    String year = "1999";
    String month = "04";
    String day = "";
    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);
    // Should
    assertEquals(expectedFirst, result.getFromDate());
    assertEquals(expectedSecond, result.getToDate());
    assertEquals(Year.parse(year), result.getYear());
    assertEquals(Month.APRIL, result.getMonth());
    assertEquals(MonthDay.from(expectedFirst).getDayOfMonth(), result.getDay().intValue());
    assertTrue(result.getIssues().isEmpty());
  }


  @Test
  public void fullLocalDateTimePeriodSkipZone2Test() {
    // State
    Temporal expectedFirst = LocalDateTime.of(1999, 4, 8, 14, 7, 0);
    Temporal expectedSecond = LocalDateTime.of(2010, 8, 3, 6, 0, 0);

    String eventDate = "1999-04-08T14:07-0600/2010-08-03T06:00-0000";
    String year = "1999";
    String month = "05";
    String day = "";
    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);
    // Should
    assertEquals(expectedFirst, result.getFromDate());
    assertEquals(expectedSecond, result.getToDate());
    assertEquals(Year.parse(year), result.getYear());
    assertEquals(Month.MAY, result.getMonth());
    assertFalse(result.getDayOpt().isPresent());
    assertEquals(1, result.getIssues().size());
    assertTrue(result.getIssues().contains(DATE_MISMATCH));
  }

  @Test
  public void localDateShortTextMonthTest() {
    // State
    Temporal expectedFirst = LocalDate.of(1999, 4, 1);

    String eventDate = "01 Apr. 1999";
    String year = "1999";
    String month = "04";
    String day = "01";
    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);
    // Should
    assertEquals(expectedFirst, result.getFromDate());
    assertFalse(result.getToOpt().isPresent());
    assertEquals(Year.parse(year), result.getYear());
    assertEquals(Month.APRIL, result.getMonth());
    assertEquals(Integer.parseInt(day), result.getDay().intValue());
    assertTrue(result.getIssues().isEmpty());
  }

  @Test
  public void localDateShortTextMistakeMonthTest() {
    // State
    Temporal expectedFirst = LocalDate.of(1999, 4, 1);

    String eventDate = "01 apr. 1999";
    String year = "1999";
    String month = "04";
    String day = "01";
    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);
    // Should
    assertEquals(expectedFirst, result.getFromDate());
    assertFalse(result.getToOpt().isPresent());
    assertEquals(Year.parse(year), result.getYear());
    assertEquals(Month.APRIL, result.getMonth());
    assertEquals(Integer.parseInt(day), result.getDay().intValue());
    assertTrue(result.getIssues().isEmpty());
  }

  @Test
  public void localDateFullTextMonthTest() {
    // State
    Temporal expectedFirst = LocalDate.of(1999, 4, 1);

    String eventDate = "01 April 1999";
    String year = "1999";
    String month = "04";
    String day = "01";
    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);
    // Should
    assertEquals(expectedFirst, result.getFromDate());
    assertFalse(result.getToOpt().isPresent());
    assertEquals(Year.parse(year), result.getYear());
    assertEquals(Month.APRIL, result.getMonth());
    assertEquals(Integer.parseInt(day), result.getDay().intValue());
    assertTrue(result.getIssues().isEmpty());
  }

  @Test
  public void localDateShortTextMonthDashTest() {
    // State
    Temporal expectedFirst = LocalDate.of(1999, 4, 1);

    String eventDate = "01-Apr-1999";
    String year = "1999";
    String month = "04";
    String day = "01";
    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);
    // Should
    assertEquals(expectedFirst, result.getFromDate());
    assertFalse(result.getToOpt().isPresent());
    assertEquals(Year.parse(year), result.getYear());
    assertEquals(Month.APRIL, result.getMonth());
    assertEquals(Integer.parseInt(day), result.getDay().intValue());
    assertTrue(result.getIssues().isEmpty());
  }

  @Test
  public void localDateShortTextMonthDateMistakeTest() {
    // State
    Temporal expectedFirst = LocalDate.of(1999, 4, 1);

    String eventDate = "ß1. Apr. 1999";
    String year = "1999";
    String month = "04";
    String day = "01";
    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);
    // Should
    assertEquals(expectedFirst, result.getFromDate());
    assertFalse(result.getToOpt().isPresent());
    assertEquals(Year.parse(year), result.getYear());
    assertEquals(Month.APRIL, result.getMonth());
    assertEquals(Integer.parseInt(day), result.getDay().intValue());
    assertTrue(result.getIssues().isEmpty());
  }

  @Test
  public void eventDateWrongYearTest() {
    // State
    Temporal expectedFirst = LocalDate.of(1999, 4, 1);

    String eventDate = "apr-99";
    String year = "1999";
    String month = "04";
    String day = "01";
    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);
    // Should
    assertEquals(expectedFirst, result.getFromDate());
    assertFalse(result.getToOpt().isPresent());
    assertEquals(Year.parse(year), result.getYear());
    assertEquals(Month.APRIL, result.getMonth());
    assertEquals(Integer.parseInt(day), result.getDay().intValue());
    assertTrue(result.getIssues().isEmpty());
  }

  @Test
  public void eventDateWrongYearMonthMistakeTest() {
    // State
    Temporal expectedFirst = LocalDate.of(1999, 4, 1);

    String eventDate = "abr-99";
    String year = "1999";
    String month = "04";
    String day = "01";
    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);
    // Should
    assertEquals(expectedFirst, result.getFromDate());
    assertFalse(result.getToOpt().isPresent());
    assertEquals(Year.parse(year), result.getYear());
    assertEquals(Month.APRIL, result.getMonth());
    assertEquals(Integer.parseInt(day), result.getDay().intValue());
  }

  @Test
  public void localDateTextMonthFirstCommaTest() {
    // State
    Temporal expectedFirst = LocalDate.of(1999, 4, 1);

    String eventDate = "April 01, 1999";
    String year = null;
    String month = null;
    String day = null;

    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);

    // Should
    assertEquals(expectedFirst, result.getFromDate());
    assertFalse(result.getToOpt().isPresent());
    assertEquals(Year.from(expectedFirst), result.getYear());
    assertEquals(Month.from(expectedFirst), result.getMonth());
    assertEquals(MonthDay.from(expectedFirst).getDayOfMonth(), result.getDay().intValue());
    assertTrue(result.getIssues().isEmpty());
  }

  @Test
  public void localDateShortTextMonthFirstTest() {
    // State
    Temporal expectedFirst = LocalDate.of(1999, 4, 1);

    String eventDate = "Apr. 1, 1999";
    String year = "1999";
    String month = "4";
    String day = "1";
    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);
    // Should
    assertEquals(expectedFirst, result.getFromDate());
    assertFalse(result.getToOpt().isPresent());
    assertEquals(Year.parse(year), result.getYear());
    assertEquals(Month.APRIL, result.getMonth());
    assertEquals(Integer.parseInt(day), result.getDay().intValue());
    assertTrue(result.getIssues().isEmpty());
  }

  @Test
  public void localDateShortTextMonthFirstDotTest() {
    // State
    Temporal expectedFirst = LocalDate.of(1999, 4, 1);

    String eventDate = "Apr. 01 1999";
    String year = "1999";
    String month = "04";
    String day = "01";
    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);
    // Should
    assertEquals(expectedFirst, result.getFromDate());
    assertFalse(result.getToOpt().isPresent());
    assertEquals(Year.parse(year), result.getYear());
    assertEquals(Month.APRIL, result.getMonth());
    assertEquals(Integer.parseInt(day), result.getDay().intValue());
    assertTrue(result.getIssues().isEmpty());
  }

  @Test
  public void localDateSlashYearLastTest() {
    // State
    Temporal expectedFirst = LocalDate.of(1999, 4, 1);

    String eventDate = "01/04/1999";
    String year = "1999";
    String month = "04";
    String day = "01";
    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);
    // Should
    assertEquals(expectedFirst, result.getFromDate());
    assertFalse(result.getToOpt().isPresent());
    assertEquals(Year.parse(year), result.getYear());
    assertEquals(Month.APRIL, result.getMonth());
    assertEquals(Integer.parseInt(day), result.getDay().intValue());
    assertTrue(result.getIssues().isEmpty());
  }

  @Test
  public void localDateSlashYearFirstTest() {
    // State
    Temporal expectedFirst = LocalDate.of(1999, 4, 1);

    String eventDate = "1999/04/01";
    String year = "1999";
    String month = "04";
    String day = "01";
    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);
    // Should
    assertEquals(expectedFirst, result.getFromDate());
    assertFalse(result.getToOpt().isPresent());
    assertEquals(Year.parse(year), result.getYear());
    assertEquals(Month.APRIL, result.getMonth());
    assertEquals(Integer.parseInt(day), result.getDay().intValue());
    assertTrue(result.getIssues().isEmpty());
  }

  @Test
  public void localDateSlashShortDateTest() {
    // State
    Temporal expectedFirst = LocalDate.of(1999, 4, 1);

    String eventDate = "1999/04/1";
    String year = "1999";
    String month = "04";
    String day = "01";
    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);
    // Should
    assertEquals(expectedFirst, result.getFromDate());
    assertFalse(result.getToOpt().isPresent());
    assertEquals(Year.parse(year), result.getYear());
    assertEquals(Month.APRIL, result.getMonth());
    assertEquals(Integer.parseInt(day), result.getDay().intValue());
    assertTrue(result.getIssues().isEmpty());
  }

  @Test
  public void localDateSlashShortMonthDateTest() {
    // State
    Temporal expectedFirst = LocalDate.of(1999, 4, 1);

    String eventDate = "1999/4/1";
    String year = "1999";
    String month = "04";
    String day = "01";
    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);
    // Should
    assertEquals(expectedFirst, result.getFromDate());
    assertFalse(result.getToOpt().isPresent());
    assertEquals(Year.parse(year), result.getYear());
    assertEquals(Month.APRIL, result.getMonth());
    assertEquals(Integer.parseInt(day), result.getDay().intValue());
    assertTrue(result.getIssues().isEmpty());
  }

  @Test
  public void localDateSlashShortMonthDate2Test() {
    // State
    Temporal expectedFirst = LocalDate.of(1999, 4, 1);

    String eventDate = "1999/4/1";
    String year = "2000";
    String month = "04";
    String day = "01";
    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);
    // Should
    assertEquals(expectedFirst, result.getFromDate());
    assertFalse(result.getToOpt().isPresent());
    assertEquals(Year.parse(year), result.getYear());
    assertEquals(Month.APRIL, result.getMonth());
    assertEquals(Integer.parseInt(day), result.getDay().intValue());
    assertEquals(1, result.getIssues().size());
    assertTrue(result.getIssues().contains(DATE_MISMATCH));
  }

  @Test
  public void localDateTextMonthFirstTest() {
    // State
    Temporal expectedFirst = LocalDate.of(1999, 4, 1);

    String eventDate = "April 01 1999";
    String year = null;
    String month = null;
    String day = null;

    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);

    // Should
    assertEquals(expectedFirst, result.getFromDate());
    assertFalse(result.getToOpt().isPresent());
    assertEquals(Year.from(expectedFirst), result.getYear());
    assertEquals(Month.from(expectedFirst), result.getMonth());
    assertEquals(MonthDay.from(expectedFirst).getDayOfMonth(), result.getDay().intValue());
    assertTrue(result.getIssues().isEmpty());
  }

  @Test
  public void localDateTimeSkipZoneTest() {
    // State
    Temporal expectedFirst = LocalDateTime.of(1999, 4, 1, 9, 33, 59);

    String eventDate = "1999-04-01T09:33:59-0300";
    String year = null;
    String month = null;
    String day = null;

    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);

    // Should
    assertEquals(expectedFirst, result.getFromDate());
    assertFalse(result.getToOpt().isPresent());
    assertEquals(Year.from(expectedFirst), result.getYear());
    assertEquals(Month.from(expectedFirst), result.getMonth());
    assertEquals(MonthDay.from(expectedFirst).getDayOfMonth(), result.getDay().intValue());
    assertTrue(result.getIssues().isEmpty());
  }

  @Test
  public void yearPeriodTest() {
    // State
    Temporal expectedFirst = Year.of(1999);
    Temporal expectedSecond = Year.of(2010);

    String eventDate = "1999/2010";
    String year = null;
    String month = null;
    String day = null;

    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);

    // Should
    assertEquals(expectedFirst, result.getFromDate());
    assertEquals(expectedSecond, result.getToDate());
    assertEquals(expectedFirst, result.getYear());
    assertFalse(result.getMonthOpt().isPresent());
    assertFalse(result.getDayOpt().isPresent());
    assertTrue(result.getIssues().isEmpty());
  }

  @Test
  public void yearEventDateTest() {
    // State
    Temporal expected = Year.of(1973);

    String eventDate = "1973";
    String year = null;
    String month = null;
    String day = null;

    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);

    // Should
    assertEquals(expected, result.getFromDate());
    assertFalse(result.getToOpt().isPresent());
    assertEquals(expected, result.getYear());
    assertFalse(result.getMonthOpt().isPresent());
    assertFalse(result.getDayOpt().isPresent());
    assertTrue(result.getIssues().isEmpty());
  }

  @Test
  public void shortYearMonthPeriodTest() {
    // State
    Temporal expectedFirst = YearMonth.of(1999, 4);
    Temporal expectedSecond = YearMonth.of(1999, 10);

    String eventDate = "1999-04/10";
    String year = null;
    String month = null;
    String day = null;

    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);

    // Should
    assertEquals(expectedFirst, result.getFromDate());
    assertEquals(Year.from(expectedFirst), result.getYear());
    assertEquals(Month.from(expectedFirst), result.getMonth());
    assertFalse(result.getDayOpt().isPresent());
    assertEquals(expectedSecond, result.getToDate());
    assertTrue(result.getIssues().isEmpty());
  }

  @Test
  public void localDatePeriodTest() {
    // State
    Temporal expectedFirst = LocalDate.of(1999, 4, 12);
    Temporal expectedSecond = LocalDate.of(2009, 10, 8);

    String eventDate = "1999-04-12/2009-10-08";
    String year = null;
    String month = null;
    String day = null;

    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);

    // Should
    assertEquals(expectedFirst, result.getFromDate());
    assertEquals(expectedSecond, result.getToDate());
    assertEquals(Year.from(expectedFirst), result.getYear());
    assertEquals(Month.from(expectedFirst), result.getMonth());
    assertEquals(MonthDay.from(expectedFirst).getDayOfMonth(), result.getDay().intValue());
    assertTrue(result.getIssues().isEmpty());
  }

  @Test
  public void localDateTimePeriodToTimeOnlyTest() {
    // State
    Temporal expectedFirst = LocalDateTime.of(1999, 4, 17, 12, 26, 0);
    Temporal expectedSecond = LocalDateTime.of(1999, 4, 17, 12, 52, 17);

    String eventDate = "1999-04-17T12:26Z/12:52:17Z";
    String year = null;
    String month = null;
    String day = null;

    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);

    // Should
    assertEquals(expectedFirst, result.getFromDate());
    assertEquals(expectedSecond, result.getToDate());
    assertEquals(Year.from(expectedFirst), result.getYear());
    assertEquals(Month.from(expectedFirst), result.getMonth());
    assertEquals(MonthDay.from(expectedFirst).getDayOfMonth(), result.getDay().intValue());
    assertTrue(result.getIssues().isEmpty());
  }

  @Test
  public void featureYearTest() {
    String eventDate = "2100";
    String year = null;
    String month = null;
    String day = null;

    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);

    // Should
    assertFalse(result.getFromOpt().isPresent());
    assertFalse(result.getToOpt().isPresent());
    assertFalse(result.getYearOpt().isPresent());
    assertFalse(result.getMonthOpt().isPresent());
    assertFalse(result.getDayOpt().isPresent());
    assertEquals(1, result.getIssues().size());
    assertTrue(result.getIssues().contains(DATE_UNLIKELY));
  }

  @Test
  public void featureYear2Test() {
    String eventDate = null;
    String year = "2100";
    String month = null;
    String day = null;

    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);

    // Should
    assertFalse(result.getFromOpt().isPresent());
    assertFalse(result.getToOpt().isPresent());
    assertFalse(result.getYearOpt().isPresent());
    assertFalse(result.getMonthOpt().isPresent());
    assertFalse(result.getDayOpt().isPresent());
    assertEquals(1, result.getIssues().size());
    assertTrue(result.getIssues().contains(DATE_UNLIKELY));
  }

  @Test
  public void yearMonthSlashMontFirstTest() {
    // State
    Temporal expectedFirst = YearMonth.of(2000, 12);

    String eventDate = "12/2000";
    String year = null;
    String month = null;
    String day = null;

    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);

    // Should
    assertEquals(expectedFirst, result.getFromDate());
    assertFalse(result.getToOpt().isPresent());
    assertEquals(Year.from(expectedFirst), result.getYear());
    assertEquals(Month.from(expectedFirst), result.getMonth());
    assertFalse(result.getDayOpt().isPresent());
    assertTrue(result.getIssues().isEmpty());
  }

  @Test
  public void yearMonthSlashTest() {
    // State
    Temporal expectedFirst = YearMonth.of(2000, 12);

    String eventDate = "2000/12";
    String year = null;
    String month = null;
    String day = null;

    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);

    // Should
    assertEquals(expectedFirst, result.getFromDate());
    assertFalse(result.getToOpt().isPresent());
    assertEquals(Year.from(expectedFirst), result.getYear());
    assertEquals(Month.from(expectedFirst), result.getMonth());
    assertFalse(result.getDayOpt().isPresent());
    assertTrue(result.getIssues().isEmpty());
  }

  @Test
  public void yearMonthPeriodTest() {
    // State
    Temporal expectedFirst = YearMonth.of(1999, 4);
    Temporal expectedSecond = YearMonth.of(2010, 1);

    String eventDate = "1999-04/2010-01";
    String year = null;
    String month = null;
    String day = null;

    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);

    // Should
    assertEquals(expectedFirst, result.getFromDate());
    assertEquals(expectedSecond, result.getToDate());
    assertEquals(Year.from(expectedFirst), result.getYear());
    assertEquals(Month.from(expectedFirst), result.getMonth());
    assertFalse(result.getDayOpt().isPresent());
    assertTrue(result.getIssues().isEmpty());
  }

  @Test
  public void localDatePeriodToMonthOnlyTest() {
    // State
    Temporal expectedFirst = LocalDate.of(1999, 4, 1);
    Temporal expectedSecond = LocalDate.of(1999, 4, 11);

    String eventDate = "1999-04-01/11";
    String year = null;
    String month = null;
    String day = null;

    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);

    // Should
    assertEquals(expectedFirst, result.getFromDate());
    assertEquals(expectedSecond, result.getToDate());
    assertEquals(Year.from(expectedFirst), result.getYear());
    assertEquals(Month.from(expectedFirst), result.getMonth());
    assertEquals(MonthDay.from(expectedFirst).getDayOfMonth(), result.getDay().intValue());
    assertTrue(result.getIssues().isEmpty());
  }

  @Test
  public void yearMonthShortMonthSlashTest() {
    // State
    Temporal expectedFirst = YearMonth.of(2000, 2);

    String eventDate = "2000/2";
    String year = null;
    String month = null;
    String day = null;

    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);

    // Should
    assertEquals(expectedFirst, result.getFromDate());
    assertFalse(result.getToOpt().isPresent());
    assertEquals(Year.from(expectedFirst), result.getYear());
    assertEquals(Month.from(expectedFirst), result.getMonth());
    assertFalse(result.getDayOpt().isPresent());
    assertTrue(result.getIssues().isEmpty());
  }

  @Test
  public void localDateTimeSkipLongZoneTest() {
    // State
    Temporal expectedFirst = LocalDateTime.of(2016, 9, 15, 0, 5, 0);

    String eventDate = "2016-09-15T00:05:00+1400 (LINT, Kiritimati, Kiribati - Christmas Island, UTC+14)";
    String year = null;
    String month = null;
    String day = null;

    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);

    // Should
    assertEquals(expectedFirst, result.getFromDate());
    assertFalse(result.getToOpt().isPresent());
    assertEquals(Year.from(expectedFirst), result.getYear());
    assertEquals(Month.from(expectedFirst), result.getMonth());
    assertEquals(MonthDay.from(expectedFirst).getDayOfMonth(), result.getDay().intValue());
    assertTrue(result.getIssues().isEmpty());
  }

  @Test
  public void localDateTimeSpaceTest() {
    // State
    Temporal expectedFirst = LocalDateTime.of(2009, 2, 13, 15, 20, 0);

    String eventDate = "2009-02-13 15:20";
    String year = null;
    String month = null;
    String day = null;

    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);

    // Should
    assertEquals(expectedFirst, result.getFromDate());
    assertFalse(result.getToOpt().isPresent());
    assertEquals(Year.from(expectedFirst), result.getYear());
    assertEquals(Month.from(expectedFirst), result.getMonth());
    assertEquals(MonthDay.from(expectedFirst).getDayOfMonth(), result.getDay().intValue());
    assertTrue(result.getIssues().isEmpty());
  }

  @Test
  public void localDateTimeDoubleSpaceTest() {
    // State
    Temporal expectedFirst = LocalDateTime.of(1987, 4, 11, 9, 30, 0);

    String eventDate = "1987-04-11  9:30";
    String year = null;
    String month = null;
    String day = null;

    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);

    // Should
    assertEquals(expectedFirst, result.getFromDate());
    assertFalse(result.getToOpt().isPresent());
    assertEquals(Year.from(expectedFirst), result.getYear());
    assertEquals(Month.from(expectedFirst), result.getMonth());
    assertEquals(MonthDay.from(expectedFirst).getDayOfMonth(), result.getDay().intValue());
    assertTrue(result.getIssues().isEmpty());
  }

  @Test
  public void localDateTimeSpaceShortHourTest() {
    // State
    Temporal expectedFirst = LocalDateTime.of(1958, 5, 5, 9, 0, 0);

    String eventDate = "1958-05-05 9:00";
    String year = null;
    String month = null;
    String day = null;

    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);

    // Should
    assertEquals(expectedFirst, result.getFromDate());
    assertFalse(result.getToOpt().isPresent());
    assertEquals(Year.from(expectedFirst), result.getYear());
    assertEquals(Month.from(expectedFirst), result.getMonth());
    assertEquals(MonthDay.from(expectedFirst).getDayOfMonth(), result.getDay().intValue());
    assertTrue(result.getIssues().isEmpty());
  }

  @Test
  public void localDateTimeMillisecondsTest() {
    // State
    Temporal expectedFirst = LocalDateTime.of(1997, 12, 15, 0, 0, 0);

    String eventDate = "1997-12-15 00:00:00.0000000";
    String year = null;
    String month = null;
    String day = null;

    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);

    // Should
    assertEquals(expectedFirst, result.getFromDate());
    assertFalse(result.getToOpt().isPresent());
    assertEquals(Year.from(expectedFirst), result.getYear());
    assertEquals(Month.from(expectedFirst), result.getMonth());
    assertEquals(MonthDay.from(expectedFirst).getDayOfMonth(), result.getDay().intValue());
    assertTrue(result.getIssues().isEmpty());
  }

  @Test
  public void textEventDateOnlyTest() {
    // State
    String eventDate = "NOTEBY J.Longino: St. 804, general collecting in canopy Basiloxylon, 30m high.";
    String year = null;
    String month = null;
    String day = null;

    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);

    // Should
    assertFalse(result.getFromOpt().isPresent());
    assertFalse(result.getToOpt().isPresent());
    assertFalse(result.getYearOpt().isPresent());
    assertFalse(result.getMonthOpt().isPresent());
    assertFalse(result.getDayOpt().isPresent());
    assertEquals(1, result.getIssues().size());
    assertTrue(result.getIssues().contains(DATE_INVALID));
  }

  @Test
  public void wrongYearOnlyTest() {
    // State
    String eventDate = null;
    String year = "0";
    String month = null;
    String day = null;

    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);
    // Should
    assertFalse(result.getFromOpt().isPresent());
    assertFalse(result.getToOpt().isPresent());
    assertFalse(result.getYearOpt().isPresent());
    assertFalse(result.getMonthOpt().isPresent());
    assertFalse(result.getDayOpt().isPresent());
    assertEquals(1, result.getIssues().size());
    assertTrue(result.getIssues().contains(DATE_UNLIKELY));
  }

  @Test
  public void localDatePeriodToMonthDayTest() {
    // State
    Temporal expectedFirst = LocalDate.of(2011, 9, 21);
    Temporal expectedSecond = LocalDate.of(2011, 10, 5);

    String eventDate = "2011-09-21/10-05";
    String year = null;
    String month = null;
    String day = null;

    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);

    // Should
    assertEquals(expectedFirst, result.getFromDate());
    assertEquals(expectedSecond, result.getToDate());
    assertEquals(Year.from(expectedFirst), result.getYear());
    assertEquals(Month.from(expectedFirst), result.getMonth());
    assertEquals(MonthDay.from(expectedFirst).getDayOfMonth(), result.getDay().intValue());
    assertTrue(result.getIssues().isEmpty());
  }

  @Test
  public void localDateNumbersOnlyTest() {
    // State
    Temporal expectedFirst = LocalDate.of(2012, 5, 6);

    String eventDate = "20120506";
    String year = null;
    String month = null;
    String day = null;

    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);

    // Should
    assertEquals(expectedFirst, result.getFromDate());
    assertFalse(result.getToOpt().isPresent());
    assertEquals(Year.from(expectedFirst), result.getYear());
    assertEquals(Month.from(expectedFirst), result.getMonth());
    assertEquals(MonthDay.from(expectedFirst).getDayOfMonth(), result.getDay().intValue());
    assertTrue(result.getIssues().isEmpty());
  }

  @Test
  public void localDateTextEventDateTest() {
    // State
    Temporal expectedFirst = LocalDate.of(1999, 1, 1);

    String eventDate = "NOTEBY J.Longino: St. 804, general collecting in canopy Basiloxylon, 30m high.";
    String year = "1999";
    String month = "1";
    String day = "1";

    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);

    // Should
    assertEquals(expectedFirst, result.getFromDate());
    assertFalse(result.getToOpt().isPresent());
    assertEquals(Year.parse(year), result.getYear());
    assertEquals(Month.JANUARY, result.getMonth());
    assertEquals(Integer.parseInt(day), result.getDay().intValue());
    assertTrue(result.getIssues().isEmpty());
  }

  @Test
  public void wrongLeapDayTest() {
    // State
    String eventDate = "2013/2/29";
    String year = null;
    String month = null;
    String day = null;

    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);

    // Should
    assertFalse(result.getFromOpt().isPresent());
    assertFalse(result.getToOpt().isPresent());
    assertFalse(result.getYearOpt().isPresent());
    assertFalse(result.getMonthOpt().isPresent());
    assertFalse(result.getDayOpt().isPresent());
    assertEquals(1, result.getIssues().size());
    assertTrue(result.getIssues().contains(DATE_INVALID));
  }

  @Test
  public void usMonthDayOrderTest() {
    // State
    Temporal expectedFirst = LocalDate.of(2013, 4, 18);

    // State
    String eventDate = "4/18/2013";
    String year = null;
    String month = null;
    String day = null;

    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);

    // Should
    assertEquals(expectedFirst, result.getFromDate());
    assertFalse(result.getToOpt().isPresent());
    assertEquals(Year.from(expectedFirst), result.getYear());
    assertEquals(Month.from(expectedFirst), result.getMonth());
    assertEquals(MonthDay.from(expectedFirst).getDayOfMonth(), result.getDay().intValue());
    assertTrue(result.getIssues().isEmpty());
  }

  @Test
  public void wrongLeapDay2Test() {
    // State
    String year = "2013";
    String month = "2";
    String day = "29";

    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, null);

    // Should
    assertFalse(result.getFromOpt().isPresent());
    assertFalse(result.getToOpt().isPresent());
    assertFalse(result.getYearOpt().isPresent());
    assertFalse(result.getMonthOpt().isPresent());
    assertFalse(result.getDayOpt().isPresent());
    assertEquals(1, result.getIssues().size());
    assertTrue(result.getIssues().contains(DATE_INVALID));
  }

  @Test
  public void wrongMonthDay2Test() {
    // State
    String year = "2013";
    String month = "11";
    String day = "31";

    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, null);

    // Should
    assertFalse(result.getFromOpt().isPresent());
    assertFalse(result.getToOpt().isPresent());
    assertFalse(result.getYearOpt().isPresent());
    assertFalse(result.getMonthOpt().isPresent());
    assertFalse(result.getDayOpt().isPresent());
    assertEquals(1, result.getIssues().size());
    assertTrue(result.getIssues().contains(DATE_INVALID));
  }

  @Test
  public void wrongMonthDayTest() {
    // State

    String eventDate = "2013/11/31";
    String year = null;
    String month = null;
    String day = null;

    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);

    // Should
    assertFalse(result.getFromOpt().isPresent());
    assertFalse(result.getToOpt().isPresent());
    assertFalse(result.getYearOpt().isPresent());
    assertFalse(result.getMonthOpt().isPresent());
    assertFalse(result.getDayOpt().isPresent());
    assertEquals(1, result.getIssues().size());
    assertTrue(result.getIssues().contains(DATE_INVALID));
  }

  @Test
  public void wrongLeapDayWithBaseTest() {
    // State
    Temporal expectedFirst = LocalDate.of(2013, 2, 28);

    String eventDate = "2013/2/29";
    String year = "2013";
    String month = "2";
    String day = "28";

    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);

    // Should
    assertEquals(expectedFirst, result.getFromDate());
    assertFalse(result.getToOpt().isPresent());
    assertEquals(Year.from(expectedFirst), result.getYear());
    assertEquals(Month.from(expectedFirst), result.getMonth());
    assertEquals(MonthDay.from(expectedFirst).getDayOfMonth(), result.getDay().intValue());
    assertTrue(result.getIssues().isEmpty());
  }

  @Test
  public void invalidPeriodTest() {

    // State
    Temporal expectedFirst = Year.of(2011);
    Temporal expectedTwo = Year.of(2013);

    String eventDate = "2013/2011";
    String year = "2013";
    String month = null;
    String day = null;

    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);

    // Should
    assertEquals(expectedFirst, result.getFromDate());
    assertEquals(expectedTwo, result.getToDate());
    assertTrue(result.getToOpt().isPresent());
    assertEquals(Year.parse(year), result.getYear());
    assertFalse(result.getMonthOpt().isPresent());
    assertFalse(result.getDayOpt().isPresent());
    assertEquals(1, result.getIssues().size());
    assertTrue(result.getIssues().contains(DATE_MISMATCH));
  }

  @Test
  public void invalidPeriod2Test() {

    // State
    Temporal expectedFirst = Year.of(2011);
    Temporal expectedSecond = Year.of(2013);

    String eventDate = "2013/2011";
    String year = null;
    String month = null;
    String day = null;

    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);

    // Should
    assertEquals(expectedFirst, result.getFromDate());
    assertEquals(expectedSecond, result.getToDate());
    assertEquals(expectedFirst, result.getYear());
    assertFalse(result.getMonthOpt().isPresent());
    assertFalse(result.getDayOpt().isPresent());
    assertTrue(result.getIssues().isEmpty());
  }
}
