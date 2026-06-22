package it.uniroma2.sae.utils;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * Utility class for date and timestamp formatting.
 */
public class DateUtils {

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter
            .ofPattern("yyyy-MM-dd HH:mm:ss")
            .withZone(ZoneId.of("UTC"));

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
}