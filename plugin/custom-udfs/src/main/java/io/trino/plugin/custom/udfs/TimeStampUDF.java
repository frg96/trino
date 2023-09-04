/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.custom.udfs;

import io.airlift.log.Logger;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.Description;
import io.trino.spi.function.LiteralParameter;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.LongTimestamp;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;

@Description("Returns input Timestamp")
@ScalarFunction("returnTimestamp")
public class TimeStampUDF
{
    private TimeStampUDF()
    {}

    static Logger logger = Logger.get(TimeStampUDF.class);
    private static final String DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";

    private static final DateTimeFormatter formatter = new DateTimeFormatterBuilder()
            .appendPattern(DATETIME_FORMAT)
            .optionalStart()
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
            .optionalEnd()
            .toFormatter();

    @LiteralParameters("p")
    @SqlNullable
    @SqlType("timestamp(p)")
    public static Long returnTimestamp0to6(
            @LiteralParameter("p") Long precision,
            ConnectorSession session,
            @SqlNullable @SqlType("timestamp(p)") Long input)
    {
        logger.info("Started method returnTimestamp0to6");
        logger.info("session Timezone: " + session.getTimeZoneKey());
        logger.info("session locale: " + session.getLocale());
        logger.info("precision: " + precision);
        logger.info("Long input (No. of microseconds since epoch): " + input);

        long secondsInput = Math.floorDiv(input, 1_000_000L);
        logger.info("input seconds: " + secondsInput);

        int microsOfSecondsInput = (int) Math.floorMod(input, 1_000_000L);
        int nanosOfSecondsInput = microsOfSecondsInput * 1000;
        logger.info("input nanosOfSeconds: " + nanosOfSecondsInput);

        //LocalDateTime
        LocalDateTime localDateTimeInput = LocalDateTime.ofEpochSecond(secondsInput, nanosOfSecondsInput, ZoneOffset.UTC);

        String localDateTimeInputStr = formatter.format(localDateTimeInput);
        logger.info("DateFormat localDateTime string: " + localDateTimeInputStr);

        // Re convert
        String localDateTimeOutputStr = localDateTimeInputStr;

        LocalDateTime localDateTimeOutput = LocalDateTime.parse(localDateTimeOutputStr, formatter);

        long secondsOutput = localDateTimeOutput.toEpochSecond(ZoneOffset.UTC);
        logger.info("output seconds: " + secondsOutput);

        int nanosOfSecondsOutput = localDateTimeOutput.getNano();
        int microsOfSecondsOutput = nanosOfSecondsOutput / 1000;

        long output = secondsOutput * 1_000_000 + microsOfSecondsOutput;
        logger.info("Long output micros: " + output);

        return output;
    }

    @LiteralParameters("p")
    @SqlNullable
    @SqlType("timestamp(p)")
    public static LongTimestamp returnTimestamp7to12(
            @LiteralParameter("p") Long precision,
            ConnectorSession session,
            @SqlNullable @SqlType("timestamp(p)") LongTimestamp input)
    {
        logger.info("Started method returnTimestamp7to12");
        logger.info("session Timezone: " + session.getTimeZoneKey());
        logger.info("session locale: " + session.getLocale());
        logger.info("precision: " + precision);
        logger.info("LongTimestamp.toString() input: " + input);
        logger.info("LongTimestamp.getEpochMicros() input: " + input.getEpochMicros());
        logger.info("LongTimestamp.getPicosOfMicro() input: " + input.getPicosOfMicro());

        long secondsInput = Math.floorDiv(input.getEpochMicros(), 1_000_000L);
        logger.info("input seconds: " + secondsInput);

        int microsOfSecondsInput = (int) Math.floorMod(input.getEpochMicros(), 1_000_000L);
        int nanosOfSecondsInput = (microsOfSecondsInput * 1000) + (input.getPicosOfMicro() / 1000);
        logger.info("input nanosOfSeconds: " + nanosOfSecondsInput);

        // backup
        int picosOfNanosInput = input.getPicosOfMicro() % 1000;

        //LocalDateTime
        LocalDateTime localDateTimeInput = LocalDateTime.ofEpochSecond(secondsInput, nanosOfSecondsInput, ZoneOffset.UTC);

        String localDateTimeInputStr = formatter.format(localDateTimeInput);
        logger.info("DateFormat localDateTime string: " + localDateTimeInputStr);

        // Re convert
        String localDateTimeOutputStr = localDateTimeInputStr;

        LocalDateTime localDateTimeOutput = LocalDateTime.parse(localDateTimeOutputStr, formatter);

        long secondsOutput = localDateTimeOutput.toEpochSecond(ZoneOffset.UTC);
        logger.info("output seconds: " + secondsOutput);

        int nanosOfSecondsOutput = localDateTimeOutput.getNano();
        int microsOfSecondsOutput = nanosOfSecondsOutput / 1000;

        long microsOutput = secondsOutput * 1_000_000 + microsOfSecondsOutput;
        logger.info("Long output micros: " + microsOutput);

        int nanosOfMicrosOutput = nanosOfSecondsOutput % 1000;
        int picosOfMicrosOutput = nanosOfMicrosOutput * 1000 + picosOfNanosInput;
        logger.info("output picosOfMicros: " + picosOfMicrosOutput);

        return new LongTimestamp(microsOutput, picosOfMicrosOutput);
    }
}

/*
trino> select returnTimestamp(TIMESTAMP '2020-06-10 15:55:23.123456');
           _col0
----------------------------
 2020-06-10 15:55:23.123456
(1 row)

Query 20230902_192005_00000_ciqgf, FINISHED, 1 node
Splits: 1 total, 1 done (100.00%)
0.35 [0 rows, 0B] [0 rows/s, 0B/s]

2023-09-03T00:50:05.542+0530    INFO    Query-20230902_192005_00000_ciqgf-206    io.trino.plugin.custom.udfs.TimeStampUDF    Started method returnTimestamp0to6
2023-09-03T00:50:05.542+0530    INFO    Query-20230902_192005_00000_ciqgf-206    io.trino.plugin.custom.udfs.TimeStampUDF    session Timezone: America/New_York
2023-09-03T00:50:05.542+0530    INFO    Query-20230902_192005_00000_ciqgf-206    io.trino.plugin.custom.udfs.TimeStampUDF    session locale: en_IN
2023-09-03T00:50:05.542+0530    INFO    Query-20230902_192005_00000_ciqgf-206    io.trino.plugin.custom.udfs.TimeStampUDF    precision: 6
2023-09-03T00:50:05.542+0530    INFO    Query-20230902_192005_00000_ciqgf-206    io.trino.plugin.custom.udfs.TimeStampUDF    Long input (No. of microseconds since epoch): 1591804523123456
----

trino> select returnTimestamp(TIMESTAMP '2020-06-10 15:55:23.123456789');
             _col0
-------------------------------
 2020-06-10 15:55:23.123456789
(1 row)

Query 20230902_192113_00001_ciqgf, FINISHED, 1 node
Splits: 1 total, 1 done (100.00%)
0.08 [0 rows, 0B] [0 rows/s, 0B/s]

2023-09-03T00:51:13.126+0530    INFO    Query-20230902_192113_00001_ciqgf-243    io.trino.plugin.custom.udfs.TimeStampUDF    Started method returnTimestamp7to12
2023-09-03T00:51:13.126+0530    INFO    Query-20230902_192113_00001_ciqgf-243    io.trino.plugin.custom.udfs.TimeStampUDF    session Timezone: America/New_York
2023-09-03T00:51:13.126+0530    INFO    Query-20230902_192113_00001_ciqgf-243    io.trino.plugin.custom.udfs.TimeStampUDF    session locale: en_IN
2023-09-03T00:51:13.126+0530    INFO    Query-20230902_192113_00001_ciqgf-243    io.trino.plugin.custom.udfs.TimeStampUDF    precision: 9
2023-09-03T00:51:13.126+0530    INFO    Query-20230902_192113_00001_ciqgf-243    io.trino.plugin.custom.udfs.TimeStampUDF    LongTimestamp.toString() input: 2020-06-10 15:55:23.123456789000
2023-09-03T00:51:13.126+0530    INFO    Query-20230902_192113_00001_ciqgf-243    io.trino.plugin.custom.udfs.TimeStampUDF    LongTimestamp.getEpochMicros() input: 1591804523123456
2023-09-03T00:51:13.127+0530    INFO    Query-20230902_192113_00001_ciqgf-243    io.trino.plugin.custom.udfs.TimeStampUDF    LongTimestamp.getPicosOfMicro() input: 789000
 */
