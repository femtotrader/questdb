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

package io.questdb.test.griffin.engine.functions.catalogue;

import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompiler;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class PgAttributeFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testCachedWindowQueryOrderedByColumnNotOnSelectList() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table y (a int, b int)");
            execute("insert into y select x/4, x from long_sequence(10)");
            engine.releaseAllWriters();

            String query = "select b.a, row_number() OVER (PARTITION BY b.a ORDER BY b.b desc) as b " +
                    " from y b " +
                    "order by b.b";

            assertPlanNoLeakCheck(query,
                    "SelectedRecord\n" +
                            "    Radix sort light\n" +
                            "      keys: [b1]\n" +
                            "        CachedWindow\n" +
                            "          orderedFunctions: [[b desc] => [row_number() over (partition by [a1])]]\n" +
                            "            SelectedRecord\n" +
                            "                PageFrame\n" +
                            "                    Row forward scan\n" +
                            "                    Frame forward scan on: y\n");

            assertQueryNoLeakCheck(
                    "a\tb\n" +
                            "0\t3\n" +
                            "0\t2\n" +
                            "0\t1\n" +
                            "1\t4\n" +
                            "1\t3\n" +
                            "1\t2\n" +
                            "1\t1\n" +
                            "2\t3\n" +
                            "2\t2\n" +
                            "2\t1\n",
                    query,
                    null,
                    true,
                    false,
                    false
            );
        });
    }

    @Test
    public void testKafkaMetadataQuery() throws Exception {
        String query = "\n" +
                "SELECT\n" +
                "    result.TABLE_CAT,        \n" +
                "    result.TABLE_SCHEM,        \n" +
                "    result.TABLE_NAME,        \n" +
                "    result.COLUMN_NAME,        \n" +
                "    result.KEY_SEQ,        \n" +
                "    result.PK_NAME,\n" +
                "    result.KEYS,\n" +
                "    result.A_ATTNUM,\n" +
                "    RAW \n" +
                "FROM\n" +
                "    (SELECT \n" +
                "        NULL AS TABLE_CAT, \n" +
                "        n.nspname AS TABLE_SCHEM,   \n" +
                "        ct.relname AS TABLE_NAME, \n" +
                "        a.attname AS COLUMN_NAME,   \n" +
                "        (information_schema._pg_expandarray(i.indkey)).n AS KEY_SEQ, \n" +
                "        ci.relname AS PK_NAME,   \n" +
                "        information_schema._pg_expandarray(i.indkey) AS KEYS, \n" +
                "        a.attnum AS A_ATTNUM,\n" +
                "        i.indkey AS RAW \n" +
                "    FROM pg_catalog.pg_class ct\n" +
                "        JOIN pg_catalog.pg_attribute a ON (ct.oid = a.attrelid)   \n" +
                "        JOIN pg_catalog.pg_namespace n ON (ct.relnamespace = n.oid)   \n" +
                "        JOIN pg_catalog.pg_index i ON ( a.attrelid = i.indrelid) \n" +
                "        JOIN pg_catalog.pg_class ci ON (ci.oid = i.indexrelid) \n" +
                "    WHERE \n" +
                "        true  \n" +
                "        AND ct.relname = E'po_items' \n" +
                "        AND i.indisprimary  \n" +
                "    ) result; \n";

        assertQuery(
                "TABLE_CAT\tTABLE_SCHEM\tTABLE_NAME\tCOLUMN_NAME\tKEY_SEQ\tPK_NAME\tKEYS\tA_ATTNUM\tRAW\n",
                query,
                "create table x(a int)",
                null,
                false,
                false
        );
    }

    @Test
    public void testKafkaMetadataQueryCaseInsensitivity1() throws Exception {
        String query = "SELECT\n" +
                "    result.TABLE_CAT,        \n" +
                "    result.TABLE_SCHEM,        \n" +
                "    result.TABLE_NAME,        \n" +
                "    result.COLUMN_NAME,        \n" +
                "    result.KEY_SEQ,        \n" +
                "    result.PK_NAME\n" +
                "FROM\n" +
                "    (SELECT \n" +
                "        NULL AS TABLE_CAT, \n" +
                "        n.nspname AS TABLE_SCHEM,   \n" +
                "        ct.relname AS TABLE_NAME, \n" +
                "        a.attname AS COLUMN_NAME,   \n" +
                "        (information_schema._pg_expandarray(i.indkey)).n AS KEY_SEQ, \n" +
                "        ci.relname AS PK_NAME,   \n" +
                "        information_schema._pg_expandarray(i.indkey) AS KEYS, \n" +
                "        a.attnum AS A_ATTNUM,\n" +
                "        i.indkey AS RAW \n" +
                "    FROM pg_catalog.pg_class ct\n" +
                "        JOIN pg_catalog.pg_attribute a ON (ct.oid = a.attrelid)   \n" +
                "        JOIN pg_catalog.pg_namespace n ON (ct.relnamespace = n.oid)   \n" +
                "        JOIN pg_catalog.pg_index i ON ( a.attrelid = i.indrelid) \n" +
                "        JOIN pg_catalog.pg_class ci ON (ci.oid = i.indexrelid) \n" +
                "    WHERE \n" +
                "        true  \n" +
                "        AND ct.relname = E'po_items' \n" +
                "        AND i.indisprimary  \n" +
                "    ) result \n" +
                "WHERE A_ATTNUM = (result.KEYS).x  \n" +
                "ORDER BY result.table_name, result.PK_NAME, result.KEY_SEQ;";

        assertQuery(
                "TABLE_CAT\tTABLE_SCHEM\tTABLE_NAME\tCOLUMN_NAME\tKEY_SEQ\tPK_NAME\n",
                query,
                "create table x(a int)",
                null,
                true
        );
    }

    @Test
    public void testKafkaMetadataQueryCaseInsensitivity2() throws Exception {
        assertMemoryLeak(() -> {
            String query = "SELECT\n" +
                    "    result.TABLE_CAT,        \n" +
                    "    result.TABLE_SCHEM,        \n" +
                    "    result.TABLE_NAME,        \n" +
                    "    result.COLUMN_NAME,        \n" +
                    "    result.KEY_SEQ,        \n" +
                    "    result.PK_NAME,\n" +
                    "    result.KEYS,\n" +
                    "    result.A_ATTNUM,\n" +
                    "    RAW \n" +
                    "FROM\n" +
                    "    (SELECT \n" +
                    "        NULL AS TABLE_CAT, \n" +
                    "        n.nspname AS TABLE_SCHEM,   \n" +
                    "        ct.relname AS TABLE_NAME, \n" +
                    "        a.attname AS COLUMN_NAME,   \n" +
                    "        (information_schema._pg_expandarray(i.indkey)).n AS KEY_SEQ, \n" +
                    "        ci.relname AS PK_NAME,   \n" +
                    "        information_schema._pg_expandarray(i.indkey) AS KEYS, \n" +
                    "        a.attnum AS A_ATTNUM,\n" +
                    "        i.indkey AS RAW \n" +
                    "    FROM pg_catalog.pg_class ct\n" +
                    "        JOIN pg_catalog.pg_attribute a ON (ct.oid = a.attrelid)   \n" +
                    "        JOIN pg_catalog.pg_namespace n ON (ct.relnamespace = n.oid)   \n" +
                    "        JOIN pg_catalog.pg_index i ON ( a.attrelid = i.indrelid) \n" +
                    "        JOIN pg_catalog.pg_class ci ON (ci.oid = i.indexrelid) \n" +
                    "    WHERE \n" +
                    "        true  \n" +
                    "        AND ct.relname = E'po_items' \n" +
                    "        AND i.indisprimary  \n" +
                    "    ) result \n" +
                    "WHERE A_ATTNUM = (result.KEYS).x  \n" +
                    "ORDER BY result.TABLE_NAME, result.pk_name, result.KEY_SEQ;";

            assertQuery(
                    "TABLE_CAT\tTABLE_SCHEM\tTABLE_NAME\tCOLUMN_NAME\tKEY_SEQ\tPK_NAME\tKEYS\tA_ATTNUM\tRAW\n",
                    query,
                    "create table x(a int)",
                    null,
                    true,
                    false
            );
        });
    }

    @Test
    public void testKafkaQuery3() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table y (a int, b short, c byte, d long, e char, f string, g boolean, h long256, i float, j double, k date, l timestamp)");
            engine.releaseAllWriters();

            assertQueryNoLeakCheck(
                    "nspname\trelname\tattname\tatttypid\tattnotnull\tatttypmod\tattlen\ttyptypmod\tattnum\tattidentity\tadsrc\tdescription\ttypbasetype\ttyptype\n" +
                            "public\ty\ta\t23\tfalse\t-1\t4\t0\t1\t\t\t\t0\tb\n" +
                            "public\ty\tb\t21\tfalse\t-1\t2\t0\t2\t\t\t\t0\tb\n" +
                            "public\ty\tc\t21\tfalse\t-1\t2\t0\t3\t\t\t\t0\tb\n" +
                            "public\ty\td\t20\tfalse\t-1\t8\t0\t4\t\t\t\t0\tb\n" +
                            "public\ty\te\t1042\tfalse\t5\t-1\t0\t5\t\t\t\t0\tb\n" +
                            "public\ty\tf\t1043\tfalse\t-1\t-1\t0\t6\t\t\t\t0\tb\n" +
                            "public\ty\tg\t16\tfalse\t-1\t1\t0\t7\t\t\t\t0\tb\n" +
                            "public\ty\th\t1043\tfalse\t-1\t-1\t0\t8\t\t\t\t0\tb\n" +
                            "public\ty\ti\t700\tfalse\t-1\t4\t0\t9\t\t\t\t0\tb\n" +
                            "public\ty\tj\t701\tfalse\t-1\t8\t0\t10\t\t\t\t0\tb\n" +
                            "public\ty\tk\t1114\tfalse\t-1\t8\t0\t11\t\t\t\t0\tb\n" +
                            "public\ty\tl\t1114\tfalse\t-1\t8\t0\t12\t\t\t\t0\tb\n",
                    "SELECT * FROM (\n" +
                            "    SELECT \n" +
                            "        n.nspname,\n" +
                            "        c.relname,\n" +
                            "        a.attname,\n" +
                            "        a.atttypid,\n" +
                            "        a.attnotnull OR (t.typtype = 'd' AND t.typnotnull) AS attnotnull,\n" +
                            "        a.atttypmod,\n" +
                            "        a.attlen,\n" +
                            "        t.typtypmod,\n" +
                            "        row_number() OVER (PARTITION BY a.attrelid ORDER BY a.attnum) AS attnum, \n" +
                            "        nullif(a.attidentity, '') as attidentity,\n" +
                            "        pg_catalog.pg_get_expr(def.adbin, def.adrelid) AS adsrc,\n" +
                            "        dsc.description,\n" +
                            "        t.typbasetype,\n" +
                            "        t.typtype  \n" +
                            "    FROM pg_catalog.pg_namespace n  \n" +
                            "        JOIN pg_catalog.pg_class c ON (c.relnamespace = n.oid)  \n" +
                            "        JOIN pg_catalog.pg_attribute a ON (a.attrelid=c.oid)  \n" +
                            "        JOIN pg_catalog.pg_type t ON (a.atttypid = t.oid)  \n" +
                            "        LEFT JOIN pg_catalog.pg_attrdef def ON (a.attrelid=def.adrelid AND a.attnum = def.adnum)  \n" +
                            "        LEFT JOIN pg_catalog.pg_description dsc ON (c.oid=dsc.objoid AND a.attnum = dsc.objsubid)  \n" +
                            "        LEFT JOIN pg_catalog.pg_class dc ON (dc.oid=dsc.classoid AND dc.relname='pg_class')  \n" +
                            "        LEFT JOIN pg_catalog.pg_namespace dn ON (dc.relnamespace=dn.oid AND dn.nspname='pg_catalog')  \n" +
                            "    WHERE \n" +
                            "        c.relkind in ('r','p','v','f','m') \n" +
                            "        and a.attnum > 0 \n" +
                            "        AND NOT a.attisdropped  \n" +
                            "        AND c.relname LIKE E'y'\n" +
                            "    ) c \n" +
                            "WHERE true  \n" +
                            "ORDER BY \n" +
                            "    nspname,\n" +
                            "    c.relname,\n" +
                            "    attnum",
                    "create table x(a int)",
                    null,
                    true,
                    false
            );
        });
    }

    @Test
    public void testKafkaQuery31() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table y (a int, b short, c byte, d long, e char, f string, g boolean, h long256, i float, j double, k date, l timestamp)");
            engine.releaseAllWriters();

            assertQueryNoLeakCheck(
                    "nspname\trelname\tattname\tatttypid\tattnotnull\tatttypmod\tattlen\ttyptypmod\tattnum\tattidentity\tadsrc\tdescription\ttypbasetype\ttyptype\n" +
                            "public\ty\ta\t23\tfalse\t-1\t4\t0\t1\t\t\t\t0\tb\n" +
                            "public\ty\tb\t21\tfalse\t-1\t2\t0\t2\t\t\t\t0\tb\n" +
                            "public\ty\tc\t21\tfalse\t-1\t2\t0\t3\t\t\t\t0\tb\n" +
                            "public\ty\td\t20\tfalse\t-1\t8\t0\t4\t\t\t\t0\tb\n" +
                            "public\ty\te\t1042\tfalse\t5\t-1\t0\t5\t\t\t\t0\tb\n" +
                            "public\ty\tf\t1043\tfalse\t-1\t-1\t0\t6\t\t\t\t0\tb\n" +
                            "public\ty\tg\t16\tfalse\t-1\t1\t0\t7\t\t\t\t0\tb\n" +
                            "public\ty\th\t1043\tfalse\t-1\t-1\t0\t8\t\t\t\t0\tb\n" +
                            "public\ty\ti\t700\tfalse\t-1\t4\t0\t9\t\t\t\t0\tb\n" +
                            "public\ty\tj\t701\tfalse\t-1\t8\t0\t10\t\t\t\t0\tb\n" +
                            "public\ty\tk\t1114\tfalse\t-1\t8\t0\t11\t\t\t\t0\tb\n" +
                            "public\ty\tl\t1114\tfalse\t-1\t8\t0\t12\t\t\t\t0\tb\n",
                    "SELECT * FROM (\n" +
                            "    SELECT \n" +
                            "        n.nspname,\n" +
                            "        c.relname,\n" +
                            "        a.attname,\n" +
                            "        a.atttypid,\n" +
                            "        a.attnotnull OR (t.typtype = 'd' AND t.typnotnull) AS attnotnull,\n" +
                            "        a.atttypmod,\n" +
                            "        a.attlen,\n" +
                            "        t.typtypmod,\n" +
                            "        row_number() OVER (PARTITION BY a.attrelid ORDER BY a.attnum) AS attnum, \n" +
                            "        nullif(a.attidentity, '') as attidentity,\n" +
                            "        pg_catalog.pg_get_expr(def.adbin, def.adrelid) AS adsrc,\n" +
                            "        dsc.description,\n" +
                            "        t.typbasetype,\n" +
                            "        t.typtype  \n" +
                            "    FROM pg_catalog.pg_namespace n  \n" +
                            "        JOIN pg_catalog.pg_class c ON (c.relnamespace = n.oid)  \n" +
                            "        JOIN pg_catalog.pg_attribute a ON (a.attrelid=c.oid)  \n" +
                            "        JOIN pg_catalog.pg_type t ON (a.atttypid = t.oid)  \n" +
                            "        LEFT JOIN pg_catalog.pg_attrdef def ON (a.attrelid=def.adrelid AND a.attnum = def.adnum)  \n" +
                            "        LEFT JOIN pg_catalog.pg_description dsc ON (c.oid=dsc.objoid AND a.attnum = dsc.objsubid)  \n" +
                            "        LEFT JOIN pg_catalog.pg_class dc ON (dc.oid=dsc.classoid AND dc.relname='pg_class')  \n" +
                            "        LEFT JOIN pg_catalog.pg_namespace dn ON (dc.relnamespace=dn.oid AND dn.nspname='pg_catalog')  \n" +
                            "    WHERE \n" +
                            "        c.relkind in ('r','p','v','f','m') \n" +
                            "        and a.attnum > 0 \n" +
                            "        AND NOT a.attisdropped  \n" +
                            "        AND c.relname LIKE E'y'\n" +
                            "    order by a.attnum" +
                            "    ) c \n",
                    "create table x(a int)",
                    null,
                    true,
                    false
            );
        });
    }

    @Test
    public void testPgAttributeFunc() throws Exception {
        assertQuery(
                "attrelid\tattname\tattnum\tatttypid\tattnotnull\tatttypmod\tattlen\tattidentity\tattisdropped\tatthasdef\n" +
                        "1\ta\t1\t23\tfalse\t-1\t4\t\tfalse\ttrue\n",
                "pg_catalog.pg_attribute;",
                "create table x(a int)",
                null,
                false
        );
    }

    @Test
    public void testDropAndRecreateTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (old int)");

            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                CompiledQuery compile = compiler.compile("select * from pg_catalog.pg_attribute", sqlExecutionContext);

                // we use a single instance of RecordCursorFactory before and after table drop
                // this mimic behavior of a query cache.
                try (RecordCursorFactory recordCursorFactory = compile.getRecordCursorFactory()) {
                    try (RecordCursor cursor = recordCursorFactory.getCursor(sqlExecutionContext)) {
                        assertCursor("attrelid\tattname\tattnum\tatttypid\tattnotnull\tatttypmod\tattlen\tattidentity\tattisdropped\tatthasdef\n" +
                                        "1\told\t1\t23\tfalse\t-1\t4\t\tfalse\ttrue\n",
                                false, true, true, cursor, recordCursorFactory.getMetadata(), false);
                    }

                    // recreate the same table again, this time with a different column
                    execute("drop table x");
                    execute("create table x (new long)");
                    drainWalQueue();

                    try (RecordCursor cursor = recordCursorFactory.getCursor(sqlExecutionContext)) {
                        // note the ID is 2 now!
                        assertCursor("attrelid\tattname\tattnum\tatttypid\tattnotnull\tatttypmod\tattlen\tattidentity\tattisdropped\tatthasdef\n" +
                                        "2\tnew\t1\t20\tfalse\t-1\t8\t\tfalse\ttrue\n",
                                false, true, true, cursor, recordCursorFactory.getMetadata(), false);
                    }
                }
            }
        });
    }

    @Test
    public void testPgAttributeFuncNoPrefix() throws Exception {
        assertQuery(
                "attrelid\tattname\tattnum\tatttypid\tattnotnull\tatttypmod\tattlen\tattidentity\tattisdropped\tatthasdef\n",
                "pg_attribute;",
                null,
                null,
                false
        );
    }

    @Test
    public void testPgAttributeFuncNoTables() throws Exception {
        assertQuery(
                "attrelid\tattname\tattnum\tatttypid\tattnotnull\tatttypmod\tattlen\tattidentity\tattisdropped\tatthasdef\n",
                "pg_catalog.pg_attribute;",
                null,
                null,
                false
        );
    }

    @Test
    public void testPgAttributeFuncWith2Tables() throws Exception {
        assertQuery("attrelid\tattname\tattnum\tatttypid\tattnotnull\tatttypmod\tattlen\tattidentity\tattisdropped\tatthasdef\n" +
                        "1\ta\t1\t23\tfalse\t-1\t4\t\tfalse\ttrue\n",
                "pg_catalog.pg_attribute order by 1;",
                "create table x(a int)",
                null,
                "create table y(a double, b string)",
                "attrelid\tattname\tattnum\tatttypid\tattnotnull\tatttypmod\tattlen\tattidentity\tattisdropped\tatthasdef\n" +
                        "1\ta\t1\t23\tfalse\t-1\t4\t\tfalse\ttrue\n" +
                        "2\ta\t1\t701\tfalse\t-1\t8\t\tfalse\ttrue\n" +
                        "2\tb\t2\t1043\tfalse\t-1\t-1\t\tfalse\ttrue\n", true, false, false);
    }

    @Test
    public void testPgAttributeFuncWith2TablesLimit1() throws Exception {
        assertQuery("attrelid\tattname\tattnum\tatttypid\tattnotnull\tatttypmod\tattlen\tattidentity\tattisdropped\tatthasdef\n" +
                "1\ta\t1\t23\tfalse\t-1\t4\t\tfalse\ttrue\n", "pg_catalog.pg_attribute order by 1 limit 1;", "create table x(a int)", null, "create table y(a double, b string)", "attrelid\tattname\tattnum\tatttypid\tattnotnull\tatttypmod\tattlen\tattidentity\tattisdropped\tatthasdef\n" +
                "1\ta\t1\t23\tfalse\t-1\t4\t\tfalse\ttrue\n", true, true, false);
    }

    @Test
    public void testSecondKafkaMetadataQuery() throws Exception {
        assertMemoryLeak(() -> {
            String query = "SELECT * FROM (\n" +
                    "    SELECT \n" +
                    "        n.nspname,\n" +
                    "        c.relname,\n" +
                    "        a.attname,\n" +
                    "        a.atttypid,\n" +
                    "        a.attnotnull OR (t.typtype = 'd' AND t.typnotnull) AS attnotnull,\n" +
                    "        a.atttypmod,\n" +
                    "        a.attlen,\n" +
                    "        t.typtypmod,\n" +
                    "     --   row_number() OVER (PARTITION BY a.attrelid ORDER BY a.attnum) AS attnum, \n" +
                    "        nullif(a.attidentity, '') as attidentity,\n" +
                    "        pg_catalog.pg_get_expr(def.adbin, def.adrelid) AS adsrc,\n" +
                    "        dsc.description,\n" +
                    "        t.typbasetype,t.typtype  \n" +
                    "    FROM pg_catalog.pg_namespace n  \n" +
                    "        JOIN pg_catalog.pg_class c ON (c.relnamespace = n.oid)  \n" +
                    "        JOIN pg_catalog.pg_attribute a ON (a.attrelid=c.oid)  \n" +
                    "        JOIN pg_catalog.pg_type t ON (a.atttypid = t.oid)  \n" +
                    "        LEFT JOIN pg_catalog.pg_attrdef def ON (a.attrelid=def.adrelid AND a.attnum = def.adnum)  \n" +
                    "        LEFT JOIN pg_catalog.pg_description dsc ON (c.oid=dsc.objoid AND a.attnum = dsc.objsubid)  \n" +
                    "        LEFT JOIN pg_catalog.pg_class dc ON (dc.oid=dsc.classoid AND dc.relname='pg_class')  \n" +
                    "        LEFT JOIN pg_catalog.pg_namespace dn ON (dc.relnamespace=dn.oid AND dn.nspname='pg_catalog')  \n" +
                    "    WHERE \n" +
                    "        c.relkind in ('r','p','v','f','m') \n" +
                    "        and a.attnum > 0 \n" +
                    "        AND NOT a.attisdropped  \n" +
                    " --       AND c.relname LIKE E'x'\n" +
                    "    ) c \n" +
                    "WHERE true  \n" +
                    "ORDER BY nspname,c.relname --,attnum";

            assertQuery(
                    "nspname\trelname\tattname\tatttypid\tattnotnull\tatttypmod\tattlen\ttyptypmod\tattidentity\tadsrc\tdescription\ttypbasetype\ttyptype\n" +
                            "public\tx\ta\t23\tfalse\t-1\t4\t0\t\t\t\t0\tb\n",
                    query,
                    "create table x(a int)",
                    null,
                    true
            );
        });
    }
}
