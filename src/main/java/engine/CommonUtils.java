package engine;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.concurrent.TimeUnit;

public class CommonUtils {

    private CommonUtils() {
    }

    public static String smartElapsed(long elapsedNano) {
        if (elapsedNano < TimeUnit.MICROSECONDS.toNanos(1)) {
            return Long.toString(elapsedNano) + " nsec";
        } else if (elapsedNano < TimeUnit.MILLISECONDS.toNanos(1)) {
            return BigDecimal.valueOf(elapsedNano).divide(BigDecimal.valueOf(TimeUnit.MICROSECONDS.toNanos(1)), elapsedNano < TimeUnit.MICROSECONDS.toNanos(10) ? 2 : 1, RoundingMode.HALF_UP).toString() + " usec";
        } else if (elapsedNano < TimeUnit.SECONDS.toNanos(1)) {
            return BigDecimal.valueOf(elapsedNano).divide(BigDecimal.valueOf(TimeUnit.MILLISECONDS.toNanos(1)), elapsedNano < TimeUnit.MILLISECONDS.toNanos(10) ? 2 : 1, RoundingMode.HALF_UP).toString() + " msec";
        } else if (elapsedNano < TimeUnit.MINUTES.toNanos(1)) {
            return BigDecimal.valueOf(elapsedNano).divide(BigDecimal.valueOf(TimeUnit.SECONDS.toNanos(1)), elapsedNano < TimeUnit.SECONDS.toNanos(10) ? 2 : 1, RoundingMode.HALF_UP).toString() + " sec";
        } else if (elapsedNano < TimeUnit.HOURS.toNanos(1)) {
            return BigDecimal.valueOf(elapsedNano).divide(BigDecimal.valueOf(TimeUnit.MINUTES.toNanos(1)), elapsedNano < TimeUnit.MINUTES.toNanos(10) ? 2 : 1, RoundingMode.HALF_UP).toString() + " min";
        } else {
            return BigDecimal.valueOf(elapsedNano).divide(BigDecimal.valueOf(TimeUnit.HOURS.toNanos(1)), elapsedNano < TimeUnit.HOURS.toNanos(10) ? 2 : 1, RoundingMode.HALF_UP).toString() + " hours";
        }
    }
}
