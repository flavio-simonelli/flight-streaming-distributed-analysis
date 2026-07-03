package it.uniroma2.sae.utils;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * Utility class for date and timestamp formatting.
 */
public class DateUtils {

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter
            .ofPattern("yyyy-MM-dd HH:mm:ss")
            .withZone(ZoneId.of("UTC"));

    private static final DateTimeFormatter PARSER_FORMATTER = DateTimeFormatter
            .ofPattern("yyyy-MM-dd HH:mm:ss");

    /**
     * Formats a Unix timestamp in milliseconds into a readable string.
     *
     * @param timestampMs the timestamp in milliseconds
     * @return the formatted date string in UTC
     */
    public static String formatTimestamp(long timestampMs) {
        return FORMATTER.format(Instant.ofEpochMilli(timestampMs));
    }

    /**
     * Formats an Instant object into a readable string.
     *
     * @param instant the Instant to format
     * @return the formatted date string in UTC
     */
    public static String formatTimestamp(Instant instant) {
        if(instant == null) throw new IllegalArgumentException("Instant cannot be null");
        return FORMATTER.format(instant);
    }

    /**
     * Parses a formatted date string in UTC into a Unix timestamp in milliseconds.
     *
     * @param timestampStr the formatted date string
     * @return the timestamp in milliseconds
     */
    public static long parseTimestamp(String timestampStr) {
        if (timestampStr == null) {
            return 0L;
        }
        LocalDateTime ldt = LocalDateTime.parse(
                timestampStr,
                PARSER_FORMATTER
        );
        return ldt.toInstant(ZoneOffset.UTC).toEpochMilli();
    }
}