package it.uniroma2.sae.utils;

public class MathUtils {

    public static final int DEFAULT_DECIMALS = 2;


    /**
     * Rounds a double value to a default number of decimal places.
     *
     * @param value the value to round
     * @return the rounded value
     */
    public static double roundDecimals(double value) {
        return roundDecimals(value, DEFAULT_DECIMALS);
    }

    /**
     * Performs a safe division of two double values, returning 0.0 if the denominator is zero or negative.
     * The result is rounded to a specified number of decimal places.
     *
     * @param numerator the numerator of the division
     * @param denominator the denominator of the division
     * @return the result of the division rounded to the specified number of decimal places, or 0.0 if the denominator is zero or negative
     */
    public static double safeDivideRounded(double numerator, double denominator) {
        return safeDivideRounded(numerator, denominator, DEFAULT_DECIMALS);
    }


    /**
     * Rounds a double value to a specified number of decimal places.
     *
     * @param value the value to round
     * @param decimals the number of decimal places to round to
     * @return the rounded value
     */
    public static double roundDecimals(double value, int decimals) {
        if (decimals <= 0) return Math.round(value);
        double multiplier = Math.pow(10, decimals);
        return Math.round(value * multiplier) / multiplier;
    }

    /**
     * Performs a safe division of two double values, returning 0.0 if the denominator is zero or negative.
     *
     * @param numerator the numerator of the division
     * @param denominator the denominator of the division
     * @return the result of the division, or 0.0 if the denominator is zero or negative
     */
    public static double safeDivide(double numerator, double denominator) {
        if (denominator <= 0) return 0.0;
        return numerator / denominator;
    }

    /**
     * Performs a safe division of two double values, returning 0.0 if the denominator is zero or negative.
     * The result is rounded to a specified number of decimal places.
     *
     * @param numerator the numerator of the division
     * @param denominator the denominator of the division
     * @param decimals the number of decimal places to round to
     * @return the result of the division rounded to the specified number of decimal places, or 0.0 if the denominator is zero or negative
     */
    public static double safeDivideRounded(double numerator, double denominator, int decimals) {
        return roundDecimals(safeDivide(numerator, denominator), decimals);
    }

    /**
     * Performs a safe division of two double values, multiplies the result by 100.0
     * to represent it as a percentage, and rounds it to a default number of decimal places.
     * If the denominator is zero or negative, it safely returns 0.0.
     *
     * @param numerator the numerator of the division (e.g., the count of specific events)
     * @param denominator the denominator of the division (e.g., the total count of events)
     * @return the percentage result of the division rounded to the default decimal places, or 0.0 if the denominator is zero or negative
     */
    public static double safeDividePercent(double numerator, double denominator) {
        return roundDecimals(safeDivide(numerator, denominator) * 100.0);
    }

}
