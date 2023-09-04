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
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;

public class DateUDF
{
    private DateUDF()
    {}

    static Logger logger = Logger.get(DateUDF.class);

    private static final String DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";

    private static final DateTimeFormatter formatter = new DateTimeFormatterBuilder()
            .appendPattern(DATETIME_FORMAT)
            .optionalStart()
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
            .optionalEnd()
            .toFormatter();

    @Description("Returns input Date")
    @ScalarFunction("returnDate")
    @SqlNullable
    @SqlType(StandardTypes.DATE)
    public static Long returnDate(
            ConnectorSession session,
            @SqlNullable @SqlType(StandardTypes.DATE) Long input)
    {
        logger.info("Started method returnDate");
        logger.info("session Timezone: " + session.getTimeZoneKey());
        logger.info("session locale: " + session.getLocale());
        logger.info("Long input (No.of Days since epoch): " + input);
        long inputSeconds = input * 24L * 60 * 60;
        logger.info("input seconds: " + inputSeconds);

        //LocalDateTime
        LocalDateTime localDateTimeInput = LocalDateTime.ofEpochSecond(inputSeconds, 0, ZoneOffset.UTC);

        String localDateTimeInputStr = formatter.format(localDateTimeInput);
        logger.info("DateFormat localDateTime string: " + localDateTimeInputStr);

        // Re convert
        String localDateTimeOutputStr = localDateTimeInputStr;

        LocalDateTime localDateTimeOutput = LocalDateTime.parse(localDateTimeOutputStr, formatter);

        long outputSeconds = localDateTimeOutput.toEpochSecond(ZoneOffset.UTC);
        logger.info("output seconds: " + outputSeconds);

        long output = outputSeconds / (24L * 60 * 60);
        logger.info("Long output: " + output);

        return output;
    }
}

/*
trino> select returnDate(DATE '2001-08-22');
   _col0
------------
 2001-08-22
(1 row)

Query 20230902_192437_00002_ciqgf, FINISHED, 1 node
Splits: 1 total, 1 done (100.00%)
0.36 [0 rows, 0B] [0 rows/s, 0B/s]

2023-09-03T00:54:37.542+0530    INFO    Query-20230902_192437_00002_ciqgf-264    io.trino.plugin.custom.udfs.DateUDF    Started method returnDate
2023-09-03T00:54:37.542+0530    INFO    Query-20230902_192437_00002_ciqgf-264    io.trino.plugin.custom.udfs.DateUDF    session Timezone: America/New_York
2023-09-03T00:54:37.542+0530    INFO    Query-20230902_192437_00002_ciqgf-264    io.trino.plugin.custom.udfs.DateUDF    session locale: en_IN
2023-09-03T00:54:37.542+0530    INFO    Query-20230902_192437_00002_ciqgf-264    io.trino.plugin.custom.udfs.DateUDF    Long input (No.of Days since epoch): 11556

 */
