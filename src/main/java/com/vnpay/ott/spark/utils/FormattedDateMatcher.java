package com.vnpay.ott.spark.utils;

import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.regex.Pattern;

/**
 * Created by SonCD on 14/05/2021
 */
public class FormattedDateMatcher {
    private static final Pattern DATE_PATTERN = Pattern.compile("^\\d{4}-\\d{2}-\\d{2}$");

    private static final Pattern NUMBER_PATTERN = Pattern.compile("^\\d{1,4}");

    public static boolean matchesDate(String date) {
        return DATE_PATTERN.matcher(date).matches();
    }

    public static boolean matchesNumber(String number) {
        return NUMBER_PATTERN.matcher(number).matches();
    }


    private static long numDate(String date) {
        String[] sd = date.split("-");

        LocalDate localDateStartDate = LocalDate.of(Integer.parseInt(sd[0]), Integer.parseInt(sd[1]), Integer.parseInt(sd[2]));
        LocalDate localDateEndDate = LocalDate.now();
        return ChronoUnit.DAYS.between(localDateStartDate, localDateEndDate);
    }


    public static long generateStartDate(String startDate) {
        if (matchesNumber(startDate)) {
            return Long.parseLong(startDate);

        } else if (matchesDate(startDate)) {
            return numDate(startDate);
        }
        return 0;
    }

    public static long generateFinishDate(String endDate) {

        if (matchesNumber(endDate)) {
            return Long.parseLong(endDate);

        } else if (FormattedDateMatcher.matchesDate(endDate)) {
            return FormattedDateMatcher.numDate(endDate) + 1;
        }
        return 1;
    }
}
