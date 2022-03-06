package org.ccreanga.awsutil.emr;

import java.time.Instant;

import static java.time.temporal.ChronoUnit.*;

enum DateInThePastType {
    HOUR1, HOUR2, HOUR4, DAY, WEEK, MONTH, YEAR;

    public static Instant pastInstant(DateInThePastType past) {
        Instant now = Instant.now();
        switch (past) {
            case HOUR1:
                return now.minus(1, HOURS);
            case HOUR2:
                return now.minus(2, HOURS);
            case HOUR4:
                return now.minus(4, HOURS);
            case DAY:
                return now.minus(1, DAYS);
            case WEEK:
                return now.minus(7, DAYS);
            case MONTH:
                return now.minus(30, DAYS);
            case YEAR:
                return now.minus(365, DAYS);
        }
        throw new IllegalArgumentException("unhandled time option");
    }
}
