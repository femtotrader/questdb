/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.test.cutlass.http.line;

import io.questdb.DefaultHttpClientConfiguration;
import io.questdb.PropertyKey;
import io.questdb.ServerMain;
import io.questdb.cairo.CairoEngine;
import io.questdb.client.Sender;
import io.questdb.cutlass.line.LineSenderException;
import io.questdb.cutlass.line.array.DoubleArray;
import io.questdb.cutlass.line.array.LongArray;
import io.questdb.cutlass.line.http.AbstractLineHttpSender;
import io.questdb.cutlass.line.http.LineHttpSenderV2;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.std.Chars;
import io.questdb.std.Misc;
import io.questdb.std.NumericException;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.microtime.TimestampFormatCompiler;
import io.questdb.std.datetime.millitime.DateFormatUtils;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.lang.reflect.Array;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static io.questdb.PropertyKey.DEBUG_FORCE_RECV_FRAGMENTATION_CHUNK_SIZE;
import static io.questdb.PropertyKey.LINE_HTTP_ENABLED;
import static io.questdb.client.Sender.PROTOCOL_VERSION_V2;

public class LineHttpSenderTest extends AbstractBootstrapTest {

    public static <T> T createDoubleArray(int... shape) {
        int[] indices = new int[shape.length];
        return buildNestedArray(ArrayDataType.DOUBLE, shape, 0, indices);
    }

    public static <T> T createLongArray(int... shape) {
        int[] indices = new int[shape.length];
        return buildNestedArray(ArrayDataType.LONG, shape, 0, indices);
    }

    public void assertSql(CairoEngine engine, CharSequence sql, CharSequence expectedResult) throws SqlException {
        StringSink sink = Misc.getThreadLocalSink();
        engine.print(sql, sink);
        if (!Chars.equals(sink, expectedResult)) {
            Assert.assertEquals(expectedResult, sink);
        }
    }

    @Override
    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testAppendErrors() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "2048"
            )) {
                serverMain.ddl("create table ex_tbl(b byte, s short, f float, d double, str string, sym symbol, u uuid, tss timestamp, " +
                        "i int, l long, ip ipv4, g geohash(4c), ts timestamp) timestamp(ts) partition by DAY WAL");

                int port = serverMain.getHttpServerPort();
                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + port)
                        .build()
                ) {
                    sender.table("ex_tbl")
                            .stringColumn("u", "foo")
                            .at(1233456, ChronoUnit.NANOS);
                    flushAndAssertError(
                            sender,
                            "Could not flush buffer",
                            "http-status=400",
                            "error in line 1: table: ex_tbl, column: u; cast error from protocol type: STRING to column type: UUID"
                    );

                    sender.table("ex_tbl")
                            .doubleColumn("b", 1234)
                            .at(1233456, ChronoUnit.NANOS);
                    flushAndAssertError(
                            sender,
                            "Could not flush buffer",
                            "http-status=400",
                            "error in line 1: table: ex_tbl, column: b; cast error from protocol type: FLOAT to column type: BYTE"
                    );

                    sender.table("ex_tbl")
                            .longColumn("b", 1024)
                            .at(1233456, ChronoUnit.NANOS);
                    flushAndAssertError(
                            sender,
                            "Could not flush buffer",
                            "http-status=400",
                            "error in line 1: table: ex_tbl, column: b; line protocol value: 1024 is out bounds of column type: BYTE"
                    );

                    sender.table("ex_tbl")
                            .doubleColumn("i", 1024.2)
                            .at(1233456, ChronoUnit.NANOS);
                    flushAndAssertError(
                            sender,
                            "Could not flush buffer",
                            "http-status=400",
                            "error in line 1: table: ex_tbl, column: i; cast error from protocol type: FLOAT to column type: INT"
                    );

                    sender.table("ex_tbl")
                            .doubleColumn("str", 1024.2)
                            .at(1233456, ChronoUnit.NANOS);
                    flushAndAssertError(
                            sender,
                            "Could not flush buffer",
                            "http-status=400",
                            "error in line 1: table: ex_tbl, column: str; cast error from protocol type: FLOAT to column type: STRING"
                    );
                }
            }
        });
    }

    @Test
    public void testAutoCreationArrayType() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "2048"
            )) {
                String tableName = "arr_auto_creation_test";
                int port = serverMain.getHttpServerPort();
                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + port)
                        .autoFlushRows(Integer.MAX_VALUE) // we'll flush manually
                        .retryTimeoutMillis(0)
                        .build()
                ) {
                    double[] arr = createDoubleArray(1);
                    sender.table(tableName)
                            .doubleArray("arr", arr)
                            .at(100000000000L, ChronoUnit.MICROS);
                    sender.flush();
                    serverMain.awaitTxn(tableName, 1);
                    serverMain.assertSql(
                            "show create table " + tableName,
                            "ddl\n" +
                                    "CREATE TABLE 'arr_auto_creation_test' ( \n" +
                                    "\tarr DOUBLE[],\n" +
                                    "\ttimestamp TIMESTAMP\n" +
                                    ") timestamp(timestamp) PARTITION BY DAY WAL\n" +
                                    "WITH maxUncommittedRows=500000, o3MaxLag=600000000us;\n");
                }
            }
        });
    }

    @Test
    public void testAutoDetectMaxNameLen() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "2048"
            )) {
                int port = serverMain.getHttpServerPort();
                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + port)
                        .maxNameLength(20)
                        .build()
                ) {
                    sender.table("table_with_long________________________________name")
                            .longColumn("column_with_long________________________________name", 1)
                            .at(100000, ChronoUnit.MICROS);
                }
                serverMain.awaitTable("table_with_long________________________________name");
                serverMain.assertSql("select * from 'table_with_long________________________________name'",
                        "column_with_long________________________________name\ttimestamp\n" +
                                "1\t1970-01-01T00:00:00.100000Z\n");
            }
        });
    }

    @Test
    public void testAutoFlush() throws Exception {
        Rnd rnd = TestUtils.generateRandom(LOG);
        TestUtils.assertMemoryLeak(() -> {
            int fragmentation = 300 + rnd.nextInt(100);
            LOG.info().$("=== fragmentation=").$(fragmentation).$();
            try (final TestServerMain serverMain = startWithEnvVariables(
                    DEBUG_FORCE_RECV_FRAGMENTATION_CHUNK_SIZE.getEnvVarName(), String.valueOf(fragmentation)
            )) {
                int httpPort = serverMain.getHttpServerPort();

                int totalCount = 50_000;
                int autoFlushRows = 1000;
                try (AbstractLineHttpSender sender = new LineHttpSenderV2("localhost", httpPort, DefaultHttpClientConfiguration.INSTANCE, null, autoFlushRows, null, null, null, 127, 0, 0, Long.MAX_VALUE)) {
                    for (int i = 0; i < totalCount; i++) {
                        if (i != 0 && i % autoFlushRows == 0) {
                            serverMain.awaitTable("table with space");
                            serverMain.assertSql("select count() from 'table with space'", "count\n" +
                                    i + "\n");
                        }
                        sender.table("table with space")
                                .symbol("tag1", "value" + i % 10)
                                .timestampColumn("tcol4", 10, ChronoUnit.HOURS)
                                .atNow();
                    }
                    serverMain.awaitTable("table with space");
                    serverMain.assertSql("select count() from 'table with space'", "count\n" +
                            totalCount + "\n");
                }
            }
        });
    }

    @Test
    public void testCanSendLong256ViaString() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "2048"
            )) {
                serverMain.start();
                serverMain.ddl("create table foo (ts TIMESTAMP, d LONG256) timestamp(ts) partition by day wal;");

                int port = serverMain.getHttpServerPort();
                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + port)
                        .build()
                ) {
                    sender.table("foo")
                            .stringColumn("d", "0xAB")
                            .at(1233456, ChronoUnit.NANOS);
                    sender.flush();

                    serverMain.awaitTxn("foo", 1);
                    serverMain.assertSql("foo;",
                            "ts\td\n" +
                                    "1970-01-01T00:00:00.001233Z\t0xab\n");

                    sender.table("foo")
                            .stringColumn("d", "0xABC")
                            .at(1233456, ChronoUnit.NANOS);

                    flushAndAssertError(
                            sender,
                            "Could not flush buffer",
                            "http-status=400",
                            "error in line 1: table: foo, column: d; cast error from protocol type: STRING to column type: LONG256"
                    );
                }
            }
        });
    }

    @Test
    public void testCancelRow() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "2048"
            )) {
                String tableName = "h2o_feet";
                int port = serverMain.getHttpServerPort();
                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + port)
                        .autoFlushRows(Integer.MAX_VALUE) // we want to flush manually
                        .autoFlushIntervalMillis(Integer.MAX_VALUE) // flush manually...
                        .build()) {

                    sender.cancelRow(); // this should be no-op

                    // this row should be inserted
                    sender.table(tableName)
                            .symbol("async", "true")
                            .doubleColumn("water_level", 3)
                            .at(Instant.parse("2024-09-09T14:38:26.361110Z"));

                    // this one is cancelled
                    sender.table(tableName)
                            .symbol("async", "true")
                            .doubleColumn("water_level", 1);
                    sender.cancelRow();

                    // and this one should be inserted again
                    sender.table(tableName)
                            .symbol("async", "true")
                            .doubleColumn("water_level", 2)
                            .at(Instant.parse("2024-09-09T14:28:26.361110Z"));

                    sender.flush();
                }

                serverMain.awaitTxn(tableName, 1);
                serverMain.assertSql("SELECT * FROM h2o_feet",
                        "async\twater_level\ttimestamp\n" +
                                "true\t2.0\t2024-09-09T14:28:26.361110Z\n" +
                                "true\t3.0\t2024-09-09T14:38:26.361110Z\n");
            }
        });
    }

    @Test
    public void testFlushAfterTimeout() throws Exception {
        // this is a regression test
        // there was a bug that flushes due to interval did not increase row count
        // bug scenario:
        // 1. flush to empty the local buffer
        // 2. period of inactivity
        // 3. insert a new row. this should trigger a flush due to interval flush. but it did not increase the row count so it stayed 0
        // 4. the flush() saw rowCount = 0 so it acted as noop
        // 5. every subsequent insert would trigger a flush due to interval (since the previous flush() was a noop and did not reset the timer)  but rowCount would stay 0
        // 6. nothing would be flushed and rows would keep accumulating in the buffer
        // 6. eventually the HTTP buffer would grow up to the limit and throw an exception

        // this test is to make sure that the scenario above does not happen
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                int httpPort = serverMain.getHttpServerPort();

                String confString = "http::addr=localhost:" + httpPort + ";auto_flush_rows=1;auto_flush_interval=1;";
                try (Sender sender = Sender.fromConfig(confString)) {

                    // insert a row to trigger row-based flush and reset the interval timer
                    sender.table("table")
                            .symbol("tag1", "value")
                            .timestampColumn("tcol4", 10, ChronoUnit.HOURS)
                            .atNow();

                    // wait a bit so the next insert triggers interval flush
                    Os.sleep(100);

                    // insert more rows
                    for (int i = 0; i < 9; i++) {
                        sender.table("table")
                                .symbol("tag1", "value")
                                .timestampColumn("tcol4", 10, ChronoUnit.HOURS)
                                .atNow();
                    }

                    serverMain.awaitTable("table");
                    serverMain.assertSql("select count() from 'table'", "count\n" +
                            10 + "\n");
                }
            }
        });
    }

    @Test
    public void testHttpWithDrop() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                int httpPort = serverMain.getHttpServerPort();

                int totalCount = 100;
                int autoFlushRows = 1000;
                String tableName = "accounts";

                try (AbstractLineHttpSender sender = new LineHttpSenderV2("localhost", httpPort, DefaultHttpClientConfiguration.INSTANCE, null, autoFlushRows, null, null, null, 127, 0, 0, Long.MAX_VALUE)) {
                    for (int i = 0; i < totalCount; i++) {
                        // Add new symbol column with each second row
                        sender.table(tableName)
                                .symbol("balance" + i / 2, String.valueOf(i))
                                .atNow();

                        sender.flush();
                    }
                }

                for (int i = 0; i < 10; i++) {
                    serverMain.ddl("drop table " + tableName);
                    assertSql(serverMain.getEngine(), "SELECT count() from tables() where table_name='" + tableName + "'", "count\n0\n");
                    serverMain.ddl("create table " + tableName + " (" +
                            "balance1 symbol capacity 16, " +
                            "balance10 symbol capacity 16, " +
                            "timestamp timestamp)" +
                            " timestamp(timestamp) partition by DAY WAL " +
                            " dedup upsert keys (balance1, balance10, timestamp)");
                    assertSql(serverMain.getEngine(), "SELECT count() FROM (table_columns('Accounts')) WHERE upsertKey=true AND ( column = 'timestamp' )", "count\n" + 1 + "\n");
                    Os.sleep(10);
                }
            }
        });
    }

    @Test
    public void testIlpWithHttpContextPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "2048",
                    PropertyKey.HTTP_CONTEXT_WEB_CONSOLE.getEnvVarName(), "context1"
            )) {
                String tableName = "h2o_feet";
                int count = 9250;

                sendIlp(tableName, count, serverMain);

                serverMain.awaitTxn(tableName, 2);
                serverMain.assertSql("SELECT count() FROM h2o_feet", "count\n" + count + "\n");
                serverMain.assertSql("SELECT sum(water_level) FROM h2o_feet", "sum\n" + (count * (count - 1) / 2) + "\n");
            }
        });
    }

    @Test
    public void testInsertDoubleArray() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "512"
            )) {
                String tableName = "arr_double_test";
                serverMain.ddl("CREATE TABLE " + tableName + " (x SYMBOL, y SYMBOL, l1 LONG, " +
                        "a1 DOUBLE[][][], a2 DOUBLE[][][], a3 DOUBLE[][][], a4 DOUBLE[][][], " +
                        "b1 DOUBLE[], b2 DOUBLE[][], b3 DOUBLE[][][], " +
                        "ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.awaitTxn(tableName, 0);

                int port = serverMain.getHttpServerPort();
                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + port)
                        .autoFlushRows(Integer.MAX_VALUE) // we want to flush manually
                        .retryTimeoutMillis(0)
                        .build();
                     DoubleArray a1 = new DoubleArray(2, 2, 2);
                     DoubleArray a2 = new DoubleArray(2, 2, 2).set(99.0, 0, 1, 0).set(100.0, 1, 1, 1);
                     DoubleArray a3 = new DoubleArray(2, 2, 2).setAll(101);
                     DoubleArray a4 = new DoubleArray(100_000_000, 100_000_000, 0).setAll(0)
                ) {
                    // array.append() appends in a circular fashion, wrapping around to start from the end.
                    // We deliberately append two more than the length of the array, to test this behavior.
                    // The intended use is to fill it up exactly, then for the next row just continue
                    // filling up with new data.
                    for (int i = 0; i < 10; i++) {
                        a1.append(i);
                    }
                    double[] arr1d = createDoubleArray(5);
                    double[][] arr2d = createDoubleArray(2, 3);
                    double[][][] arr3d = createDoubleArray(1, 2, 3);

                    sender.table(tableName)
                            .symbol("x", "42i")
                            .symbol("y", "[6f1.0,2.5,3.0,4.5,5.0]")  // ensuring no array parsing for symbol
                            .longColumn("l1", 23452345)
                            .doubleArray("a1", a1)
                            .doubleArray("a2", a2)
                            .doubleArray("a3", a3)
                            .doubleArray("a4", a4)
                            .doubleArray("b1", arr1d)
                            .doubleArray("b2", arr2d)
                            .doubleArray("b3", arr3d)
                            .at(100000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }
                serverMain.awaitTxn(tableName, 1);
                serverMain.assertSql("select * from " + tableName, "x\ty\tl1\ta1\ta2\ta3\ta4\tb1\tb2\tb3\tts\n" +
                        "42i\t[6f1.0,2.5,3.0,4.5,5.0]\t23452345\t" +
                        "[[[8.0,9.0],[2.0,3.0]],[[4.0,5.0],[6.0,7.0]]]\t" +
                        "[[[0.0,0.0],[99.0,0.0]],[[0.0,0.0],[0.0,100.0]]]\t" +
                        "[[[101.0,101.0],[101.0,101.0]],[[101.0,101.0],[101.0,101.0]]]\t" +
                        "[]\t" +
                        "[1.0,2.0,3.0,4.0,5.0]\t" +
                        "[[1.0,2.0,3.0],[2.0,4.0,6.0]]\t" +
                        "[[[1.0,2.0,3.0],[2.0,4.0,6.0]]]\t" +
                        "1970-01-02T03:46:40.000000Z\n");
            }
        });
    }

    @Test
    public void testInsertInvalidArrayDims() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "2048"
            )) {
                String tableName = "arr_exception_test";
                serverMain.ddl("CREATE TABLE " + tableName + " (x SYMBOL, a1 DOUBLE[], ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.awaitTxn(tableName, 0);

                int port = serverMain.getHttpServerPort();
                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + port)
                        .protocolVersion(PROTOCOL_VERSION_V2)
                        .autoFlushRows(Integer.MAX_VALUE)
                        .retryTimeoutMillis(0)
                        .build()
                ) {
                    sender.table(tableName)
                            .symbol("x", "42i")
                            .doubleArray("a1", (double[][]) createDoubleArray(2, 2))
                            .at(100000000000L, ChronoUnit.MICROS);
                    flushAndAssertError(
                            sender,
                            "ast error from protocol type: DOUBLE[][] to column type: DOUBLE[]");
                }

            }
        });
    }

    @Test
    public void testInsertLargeArray() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "2048"
            )) {
                String tableName = "arr_large_test";
                serverMain.ddl("CREATE TABLE " + tableName + " (ts TIMESTAMP, arr DOUBLE[])" +
                        " TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.awaitTxn(tableName, 0);

                int port = serverMain.getHttpServerPort();
                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + port)
                        .autoFlushRows(Integer.MAX_VALUE) // we'll flush manually
                        .retryTimeoutMillis(0)
                        .build()
                ) {
                    double[] arr = createDoubleArray(10_000_000);
                    sender.table(tableName)
                            .doubleArray("arr", arr)
                            .at(100000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTxn(tableName, 1);

                serverMain.assertSql(
                        "SELECT ts, arr[1] arr0, arr[10_000] arr4, arr[1_000_000] arr6, arr[10_000_000] arr7" +
                                " FROM " + tableName,
                        "ts\tarr0\tarr4\tarr6\tarr7\n" +
                                "1970-01-02T03:46:40.000000Z\t1.0\t10000.0\t1000000.0\t1.0E7\n");
            }
        });
    }

    @Test
    @Ignore("as of now only double arrays are supported")
    public void testInsertLongArray() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "512"
            )) {
                String tableName = "arr_long_test";
                serverMain.ddl("CREATE TABLE " + tableName + " (x SYMBOL, y SYMBOL, l1 LONG, " +
                        "a1 LONG[][][], a2 LONG[][][], a3 LONG[][][], a4 LONG[][][], " +
                        "b1 LONG[], b2 LONG[][], b3 LONG[][][], " +
                        "ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.awaitTxn(tableName, 0);

                int port = serverMain.getHttpServerPort();
                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + port)
                        .autoFlushRows(Integer.MAX_VALUE) // we want to flush manually
                        .retryTimeoutMillis(0)
                        .build();
                     LongArray a1 = new LongArray(2, 2, 2);
                     LongArray a2 = new LongArray(2, 2, 2).set(99L, 0, 1, 0).set(100L, 1, 1, 1);
                     LongArray a3 = new LongArray(2, 2, 2).setAll(101);
                     LongArray a4 = new LongArray(100_000_000, 100_000_000, 0)
                ) {
                    // array.append() appends in a circular fashion, wrapping around to start from the end.
                    // We deliberately append two more than the length of the array, to test this behavior.
                    // The intended use is to fill it up exactly, then for the next row just continue
                    // filling up with new data.
                    for (int i = 0; i < 10; i++) {
                        a1.append(i);
                    }

                    long[] arr1d = createLongArray(5);
                    long[][] arr2d = createLongArray(2, 3);
                    long[][][] arr3d = createLongArray(1, 2, 3);
                    sender.table(tableName)
                            .symbol("x", "42i")
                            .symbol("y", "[6f1.0,2.5,3.0,4.5,5.0]")  // ensuring no array parsing for symbol
                            .longColumn("l1", 23452345)
                            .longArray("a1", a1)
                            .longArray("a2", a2)
                            .longArray("a3", a3)
                            .longArray("a4", a4)
                            .longArray("b1", arr1d)
                            .longArray("b2", arr2d)
                            .longArray("b3", arr3d)
                            .at(100000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTxn(tableName, 1);

                serverMain.assertSql("select * from " + tableName, "x\ty\tl1\ta1\ta2\ta3\ta4\tb1\tb2\tb3\tts\n" +
                        "42i\t[6f1.0,2.5,3.0,4.5,5.0]\t23452345\t" +
                        "[[[8,9],[2,3]],[[4,5],[6,7]]]\t" +
                        "[[[0,0],[99,0]],[[0,0],[0,100]]]\t" +
                        "[[[101,101],[101,101]],[[101,101],[101,101]]]\t" +
                        "[]\t" +
                        "[1,2,3,4,5]\t" +
                        "[[1,2,3],[2,4,6]]\t" +
                        "[[[1,2,3],[2,4,6]]]\t" +
                        "1970-01-02T03:46:40.000000Z\n");
            }
        });
    }

    @Test
    public void testInsertNullArray() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "2048"
            )) {
                String tableName = "arr_nullable_test";
                serverMain.ddl("CREATE TABLE " + tableName + " (x SYMBOL, l1 LONG, a1 DOUBLE[], " +
                        "a2 DOUBLE[][], ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.awaitTxn(tableName, 0);

                int port = serverMain.getHttpServerPort();
                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + port)
                        .protocolVersion(PROTOCOL_VERSION_V2)
                        .autoFlushRows(Integer.MAX_VALUE) // we want to flush manually
                        .retryTimeoutMillis(0)
                        .build()
                ) {
                    sender.table(tableName)
                            .symbol("x", "42i")
                            .longColumn("l1", 123098948)
                            .doubleArray("a1", (double[]) null)
                            .doubleArray("a2", (double[][]) null)
                            .at(100000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTxn(tableName, 1);

                serverMain.assertSql("select * from " + tableName,
                        "x\tl1\ta1\ta2\tts\n" +
                                "42i\t123098948\tnull\tnull\t1970-01-02T03:46:40.000000Z\n");
            }
        });
    }

    @Test
    public void testInsertSimpleDouble() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "512"
            )) {
                String tableName = "simple_double_test";
                serverMain.ddl("CREATE TABLE " + tableName + " (x SYMBOL, y SYMBOL, l1 LONG, " +
                        "a double, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.awaitTxn(tableName, 0);

                int port = serverMain.getHttpServerPort();
                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + port)
                        .autoFlushRows(Integer.MAX_VALUE) // we want to flush manually
                        .retryTimeoutMillis(0)
                        .build()
                ) {
                    sender.table(tableName)
                            .symbol("x", "42i")
                            .symbol("y", "[6f1.0,2.5,3.0,4.5,5.0]")
                            .longColumn("l1", 23452345)
                            .doubleColumn("a", 1.0)
                            .at(100000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }
                serverMain.awaitTxn(tableName, 1);
                serverMain.assertSql("select * from " + tableName, "x\ty\tl1\ta\tts\n" +
                        "42i\t[6f1.0,2.5,3.0,4.5,5.0]\t23452345\t1.0\t1970-01-02T03:46:40.000000Z\n");
            }
        });
    }

    @Test
    public void testInsertWithIlpHttp() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "2048"
            )) {
                String tableName = "h2o_feet";
                int count = 9250;

                sendIlp(tableName, count, serverMain);

                serverMain.awaitTxn(tableName, 2);
                serverMain.assertSql("SELECT count() FROM h2o_feet", "count\n" + count + "\n");
                serverMain.assertSql("SELECT sum(water_level) FROM h2o_feet", "sum\n" + (count * (count - 1) / 2) + "\n");
            }
        });
    }

    @Test
    public void testInsertWithIlpHttpServerKeepAliveOff() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "2048",
                    PropertyKey.HTTP_SERVER_KEEP_ALIVE.getEnvVarName(), "false"
            )) {
                String tableName = "h2o_feet";
                int count = 9250;

                sendIlp(tableName, count, serverMain);

                serverMain.awaitTxn(tableName, 2);
                serverMain.assertSql("SELECT count() FROM h2o_feet", "count\n" + count + "\n");
                serverMain.assertSql("SELECT sum(water_level) FROM h2o_feet", "sum\n" + (count * (count - 1) / 2) + "\n");
            }
        });
    }

    @Test
    public void testInsertWithIlpHttp_varcharColumn() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "2048"
            )) {
                String tableName = "h2o_feet";
                serverMain.ddl("create table " + tableName + " (async symbol, location symbol, level varchar, water_level long, ts timestamp) timestamp(ts) partition by DAY WAL");

                int count = 10;

                String fullString = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
                int port = serverMain.getHttpServerPort();
                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + port)
                        .autoFlushRows(Integer.MAX_VALUE) // we want to flush manually
                        .build()
                ) {
                    for (int i = 0; i < count; i++) {
                        sender.table(tableName)
                                .symbol("async", "true")
                                .symbol("location", "santa_monica")
                                .stringColumn("level", fullString.substring(0, i % 10 + 10))
                                .longColumn("water_level", i)
                                .atNow();
                    }
                    sender.flush();
                }
                StringBuilder expectedString = new StringBuilder("level\n");
                for (int i = 0; i < count; i++) {
                    expectedString.append(fullString, 0, i % 10 + 10).append('\n');
                }

                serverMain.awaitTxn(tableName, 1);
                serverMain.assertSql("SELECT level FROM h2o_feet", expectedString.toString());
            }
        });
    }

    @Test
    public void testLineHttpDisabled() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    LINE_HTTP_ENABLED.getEnvVarName(), "false"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                int totalCount = 1_000;
                try (AbstractLineHttpSender sender = new LineHttpSenderV2("localhost", httpPort, DefaultHttpClientConfiguration.INSTANCE, null, 100_000, null, null, null, 127, 0, 0, Long.MAX_VALUE)) {
                    for (int i = 0; i < totalCount; i++) {
                        sender.table("table")
                                .longColumn("lcol1", i)
                                .atNow();
                    }
                    try {
                        sender.flush();
                        Assert.fail("Expected exception");
                    } catch (LineSenderException e) {
                        TestUtils.assertContains(e.getMessage(), "http-status=405");
                        TestUtils.assertContains(e.getMessage(), "Could not flush buffer: HTTP endpoint does not support ILP.");
                    }
                }
            }
        });
    }

    @Test
    public void testNegativeDesignatedTimestampDoesNotRetry() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "2048"
            )) {
                int port = serverMain.getHttpServerPort();
                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + port)
                        .retryTimeoutMillis(Integer.MAX_VALUE) // high-enoung value so the test times out if retry is attempted
                        .build()
                ) {
                    sender.table("tab")
                            .longColumn("l", 1) // filler
                            .at(-1, ChronoUnit.MICROS);
                    flushAndAssertError(
                            sender,
                            "Could not flush buffer",
                            "error in line 1: table: tab, timestamp: -1; designated timestamp before 1970-01-01 is not allowed"
                    );
                }
            }
        });
    }

    @Test
    public void testRestrictedCreateColumnsError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "2048",
                    PropertyKey.LINE_AUTO_CREATE_NEW_COLUMNS.getEnvVarName(), "false"
            )) {
                serverMain.ddl("create table ex_tbl(b byte, s short, f float, d double, str string, sym symbol, tss timestamp, " +
                        "i int, l long, ip ipv4, g geohash(4c), ts timestamp) timestamp(ts) partition by DAY WAL");

                int port = serverMain.getHttpServerPort();
                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + port)
                        .build()
                ) {
                    sender.table("ex_tbl")
                            .symbol("a3", "3")
                            .at(1222233456, ChronoUnit.NANOS);
                    flushAndAssertError(
                            sender,
                            "Could not flush buffer",
                            "http-status=400",
                            "error in line 1: table: ex_tbl, column: a3 does not exist, creating new columns is disabled"
                    );

                    sender.table("ex_tbl2")
                            .doubleColumn("d", 2)
                            .at(1222233456, ChronoUnit.NANOS);
                    flushAndAssertError(
                            sender,
                            "Could not flush buffer",
                            "http-status=400",
                            "error in line 1: table: ex_tbl2; table does not exist, cannot create table, creating new columns is disabled"
                    );
                }
            }
        });
    }

    @Test
    public void testRestrictedCreateTableError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "2048",
                    PropertyKey.LINE_AUTO_CREATE_NEW_COLUMNS.getEnvVarName(), "false",
                    PropertyKey.LINE_AUTO_CREATE_NEW_TABLES.getEnvVarName(), "false"
            )) {
                serverMain.ddl("create table ex_tbl(b byte, s short, f float, d double, str string, sym symbol, tss timestamp, " +
                        "i int, l long, ip ipv4, g geohash(4c), ts timestamp) timestamp(ts) partition by DAY WAL");

                int port = serverMain.getHttpServerPort();
                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + port)
                        .build()
                ) {
                    sender.table("ex_tbl")
                            .symbol("a3", "2")
                            .at(1222233456, ChronoUnit.NANOS);
                    flushAndAssertError(
                            sender,
                            "Could not flush buffer",
                            "http-status=400",
                            "error in line 1: table: ex_tbl, column: a3 does not exist, creating new columns is disabled"
                    );

                    sender.table("ex_tbl2")
                            .doubleColumn("d", 2)
                            .at(1222233456, ChronoUnit.NANOS);
                    flushAndAssertError(
                            sender,
                            "Could not flush buffer",
                            "http-status=400",
                            "error in line 1: table: ex_tbl2; table does not exist, creating new tables is disabled"
                    );
                }
            }
        });
    }

    @Test
    public void testSmallMaxNameLen() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                    .address("localhost:1123")
                    .retryTimeoutMillis(Integer.MAX_VALUE)
                    .maxNameLength(20)
                    .protocolVersion(PROTOCOL_VERSION_V2)
                    .build()
            ) {
                try {
                    sender.table("table_with_long______________________name");
                    Assert.fail("Expected exception");
                } catch (LineSenderException e) {
                    TestUtils.assertContains(e.getMessage(), "table name is too long: [name = table_with_long______________________name, maxNameLength=20]");
                }

                try {
                    sender.table("table")
                            .doubleColumn("column_with_long______________________name", 1.0);
                    Assert.fail("Expected exception");
                } catch (LineSenderException e) {
                    TestUtils.assertContains(e.getMessage(), "column name is too long: [name = column_with_long______________________name, maxNameLength=20]");
                }
            }
        });
    }

    @Test
    public void testSmoke() throws Exception {
        Rnd rnd = TestUtils.generateRandom(LOG);
        TestUtils.assertMemoryLeak(() -> {
            int fragmentation = 300 + rnd.nextInt(100);
            LOG.info().$("=== fragmentation=").$(fragmentation).$();
            try (final TestServerMain serverMain = startWithEnvVariables(
                    DEBUG_FORCE_RECV_FRAGMENTATION_CHUNK_SIZE.getEnvVarName(), String.valueOf(fragmentation)
            )) {
                int httpPort = serverMain.getHttpServerPort();

                int totalCount = 100_000;
                try (AbstractLineHttpSender sender = new LineHttpSenderV2("localhost", httpPort, DefaultHttpClientConfiguration.INSTANCE, null, 100_000, null, null, null, 127, 0, 0, Long.MAX_VALUE)) {
                    for (int i = 0; i < totalCount; i++) {
                        sender.table("table with space")
                                .symbol("tag1", "value" + i % 10)
                                .symbol("tag2", "value " + i % 10)
                                .stringColumn("scol1", "value" + i)
                                .stringColumn("scol2", "value" + i)
                                .longColumn("lcol 1", i)
                                .longColumn("lcol2", i)
                                .doubleColumn("dcol1", i)
                                .doubleColumn("dcol2", i)
                                .boolColumn("bcol1", i % 2 == 0)
                                .boolColumn("bcol2", i % 2 == 0)
                                .timestampColumn("tcol1", Instant.now())
                                .timestampColumn("tcol2", Instant.now())
                                .timestampColumn("tcol3", 1, ChronoUnit.HOURS)
                                .timestampColumn("tcol4", 10, ChronoUnit.HOURS)
                                .atNow();
                    }
                    sender.flush();
                }
                serverMain.awaitTable("table with space");
                serverMain.assertSql("select count() from 'table with space'", "count\n" +
                        totalCount + "\n");
            }
        });
    }

    @Test
    public void testTimestampUpperBounds() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            TimestampFormatCompiler timestampFormatCompiler = new TimestampFormatCompiler();

            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "2048"
            )) {
                serverMain.ddl("create table tab (ts timestamp, ts2 timestamp) timestamp(ts) partition by DAY WAL");

                int port = serverMain.getHttpServerPort();
                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + port)
                        .build()
                ) {

                    DateFormat format = timestampFormatCompiler.compile("yyyy-MM-dd HH:mm:ss.SSSUUU");
                    // technically, we the storage layer supports dates up to 294247-01-10T04:00:54.775807Z
                    // but DateFormat does reliably support only 4 digit years. thus we use 9999-12-31T23:59:59.999Z
                    // is the maximum date that can be reliably worked with.
                    long nonDsTs = format.parse("9999-12-31 23:59:59.999999", DateFormatUtils.EN_LOCALE);
                    long dsTs = format.parse("9999-12-31 23:59:59.999999", DateFormatUtils.EN_LOCALE);

                    // first try with ChronoUnit
                    sender.table("tab")
                            .timestampColumn("ts2", nonDsTs, ChronoUnit.MICROS)
                            .at(dsTs, ChronoUnit.MICROS);
                    sender.flush();
                    serverMain.awaitTable("tab");
                    serverMain.assertSql("SELECT * FROM tab", "ts\tts2\n" +
                            "9999-12-31T23:59:59.999999Z\t9999-12-31T23:59:59.999999Z\n");


                    // now try with the Instant overloads of `at()` and `timestampColumn()`
                    Instant nonDsInstant = Instant.ofEpochSecond(nonDsTs / 1_000_000, (nonDsTs % 1_000_000) * 1_000);
                    Instant dsInstant = Instant.ofEpochSecond(dsTs / 1_000_000, (nonDsTs % 1_000_000) * 1_000);
                    sender.table("tab")
                            .timestampColumn("ts2", nonDsInstant)
                            .at(dsInstant);
                    sender.flush();

                    serverMain.awaitTable("tab");
                    serverMain.assertSql("SELECT * FROM tab", "ts\tts2\n" +
                            "9999-12-31T23:59:59.999999Z\t9999-12-31T23:59:59.999999Z\n" +
                            "9999-12-31T23:59:59.999999Z\t9999-12-31T23:59:59.999999Z\n");
                }
            }
        });
    }

    @SuppressWarnings("unchecked")
    private static <T> T buildNestedArray(ArrayDataType dataType, int[] shape, int currentDim, int[] indices) {
        if (currentDim == shape.length - 1) {
            Object arr = dataType.createArray(shape[currentDim]);
            for (int i = 0; i < Array.getLength(arr); i++) {
                indices[currentDim] = i;
                dataType.setElement(arr, i, indices);
            }
            return (T) arr;
        } else {
            Class<?> componentType = dataType.getComponentType(shape.length - currentDim - 1);
            Object arr = Array.newInstance(componentType, shape[currentDim]);
            for (int i = 0; i < shape[currentDim]; i++) {
                indices[currentDim] = i;
                Object subArr = buildNestedArray(dataType, shape, currentDim + 1, indices);
                Array.set(arr, i, subArr);
            }
            return (T) arr;
        }
    }

    private static void flushAndAssertError(Sender sender, String... errors) {
        try {
            sender.flush();
            Assert.fail("Expected exception");
        } catch (LineSenderException e) {
            for (String error : errors) {
                TestUtils.assertContains(e.getMessage(), error);
            }
        }
    }

    private static void sendIlp(String tableName, int count, ServerMain serverMain) throws NumericException {
        long timestamp = IntervalUtils.parseFloorPartialTimestamp("2023-11-27T18:53:24.834Z");
        int i = 0;

        int port = serverMain.getHttpServerPort();
        try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                .address("localhost:" + port)
                .autoFlushRows(Integer.MAX_VALUE) // we want to flush manually
                .autoFlushIntervalMillis(Integer.MAX_VALUE) // flush manually...
                .build()
        ) {
            if (count / 2 > 0) {
                String tableNameUpper = tableName.toUpperCase();
                for (; i < count / 2; i++) {
                    String tn = i % 2 == 0 ? tableName : tableNameUpper;
                    sender.table(tn)
                            .symbol("async", "true")
                            .symbol("location", "santa_monica")
                            .stringColumn("level", "below 3 feet asd fasd fasfd asdf asdf asdfasdf asdf asdfasdfas dfads".substring(0, i % 68))
                            .longColumn("water_level", i)
                            .at(timestamp, ChronoUnit.MICROS);
                }
                sender.flush();
            }

            for (; i < count; i++) {
                String tableNameUpper = tableName.toUpperCase();
                String tn = i % 2 == 0 ? tableName : tableNameUpper;
                sender.table(tn)
                        .symbol("async", "true")
                        .symbol("location", "santa_monica")
                        .stringColumn("level", "below 3 feet asd fasd fasfd asdf asdf asdfasdf asdf asdfasdfas dfads".substring(0, i % 68))
                        .longColumn("water_level", i)
                        .at(timestamp, ChronoUnit.MICROS);
            }
            sender.flush();
        }
    }

    private enum ArrayDataType {
        DOUBLE(double.class) {
            @Override
            public Object createArray(int length) {
                return new double[length];
            }

            @Override
            public void setElement(Object array, int index, int[] indices) {
                double[] arr = (double[]) array;
                double product = 1.0;
                for (int idx : indices) {
                    product *= (idx + 1);
                }
                arr[index] = product;
            }
        },
        LONG(long.class) {
            @Override
            public Object createArray(int length) {
                return new long[length];
            }

            @Override
            public void setElement(Object array, int index, int[] indices) {
                long[] arr = (long[]) array;
                long product = 1L;
                for (int idx : indices) {
                    product *= (idx + 1);
                }
                arr[index] = product;
            }
        };

        private final Class<?> baseType;
        private final Class<?>[] componentTypes = new Class<?>[17]; // 支持最多16维

        ArrayDataType(Class<?> baseType) {
            this.baseType = baseType;
            initComponentTypes();
        }

        public abstract Object createArray(int length);

        public Class<?> getComponentType(int dimsRemaining) {
            if (dimsRemaining < 0 || dimsRemaining > 16) {
                throw new RuntimeException("Array dimension too large");
            }
            return componentTypes[dimsRemaining];
        }

        public abstract void setElement(Object array, int index, int[] indices);

        private void initComponentTypes() {
            componentTypes[0] = baseType;
            for (int dim = 1; dim <= 16; dim++) {
                componentTypes[dim] = Array.newInstance(componentTypes[dim - 1], 0).getClass();
            }
        }
    }
}
