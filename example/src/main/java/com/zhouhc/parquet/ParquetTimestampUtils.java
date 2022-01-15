package com.zhouhc.parquet;

import java.util.concurrent.TimeUnit;

import org.apache.parquet.io.api.Binary;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;

public class ParquetTimestampUtils {

    /**
     * julian date的偏移量，2440588相当于1970/1/1
     */
    private static final int JULIAN_EPOCH_OFFSET_DAYS = 2440588;
    private static final long MILLIS_IN_DAY = TimeUnit.DAYS.toMillis(1);
    private static final long NANOS_PER_MILLISECOND = TimeUnit.MILLISECONDS.toNanos(1);

    private ParquetTimestampUtils() {}

    /**
     * Returns GMT timestamp from binary encoded parquet timestamp
     (12 bytes - julian date + time of day nanos).
     *
     * @param timestampBinary INT96 parquet timestamp
     * @return timestamp in millis, GMT timezone
     */
    public static long getTimestampMillis(Binary timestampBinary)
    {
        if (timestampBinary.length() != 12) {
            return 0;
//            throw new PrestoException(HIVE_BAD_DATA,
//          "Parquet timestamp must be 12 bytes, actual " + timestampBinary.length());
        }
        byte[] bytes = timestampBinary.getBytes();

        // little endian encoding - need to invert byte order
        long timeOfDayNanos = Longs.fromBytes(bytes[7], bytes[6],
                bytes[5], bytes[4], bytes[3], bytes[2], bytes[1], bytes[0]);
        int julianDay = Ints.fromBytes(bytes[11], bytes[10], bytes[9], bytes[8]);

        return julianDayToMillis(julianDay) + (timeOfDayNanos / NANOS_PER_MILLISECOND);
    }

    private static long julianDayToMillis(int julianDay)
    {
        return (julianDay - JULIAN_EPOCH_OFFSET_DAYS) * MILLIS_IN_DAY;
    }
}
