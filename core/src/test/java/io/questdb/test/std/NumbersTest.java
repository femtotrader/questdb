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

package io.questdb.test.std;

import io.questdb.std.IntList;
import io.questdb.std.Long256FromCharSequenceDecoder;
import io.questdb.std.Long256Impl;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Random;

public class NumbersTest {
    private final StringSink sink = new StringSink();
    private Rnd rnd;

    @Test
    public void appendHexPadded() {
        sink.clear();
        Numbers.appendHexPadded(sink, 0xff - 1, 2);
        TestUtils.assertEquals("00fe", sink);

        sink.clear();
        Numbers.appendHexPadded(sink, 0xff0, 4);
        TestUtils.assertEquals("00000ff0", sink);

        sink.clear();
        Numbers.appendHexPadded(sink, 1, 4);
        TestUtils.assertEquals("00000001", sink);

        sink.clear();
        Numbers.appendHexPadded(sink, 0xff - 1, 3);
        TestUtils.assertEquals("0000fe", sink);

        sink.clear();
        Numbers.appendHexPadded(sink, 0xff - 1, 1);
        TestUtils.assertEquals("fe", sink);

        sink.clear();
        Numbers.appendHexPadded(sink, 0xffff, 0);
        TestUtils.assertEquals("ffff", sink);

        sink.clear();
        Numbers.appendHexPadded(sink, 0, 8);
        TestUtils.assertEquals("0000000000000000", sink);
    }

    @Test(expected = NumericException.class)
    public void parseExplicitDouble2() throws Exception {
        Numbers.parseDouble("1234dx");
    }

    @Test
    public void parseExplicitLong() throws Exception {
        Assert.assertEquals(10000L, Numbers.parseLong("10000L"));
    }

    @Test(expected = NumericException.class)
    public void parseExplicitLong2() throws Exception {
        Numbers.parseLong("10000LL");
    }

    @Before
    public void setUp() {
        rnd = new Rnd();
        sink.clear();
    }

    @Test
    public void testAppendZeroLong256() {
        sink.clear();
        Numbers.appendLong256(0, 0, 0, 0, sink);
        TestUtils.assertEquals("0x00", sink);
    }

    @Test
    public void testBswap() {
        int expected = rnd.nextInt();
        int x = Numbers.bswap(expected);
        Assert.assertEquals(expected, Numbers.bswap(x));
    }

    @Test
    public void testCeilPow2() {
        Assert.assertEquals(16, Numbers.ceilPow2(15));
        Assert.assertEquals(16, Numbers.ceilPow2(16));
        Assert.assertEquals(32, Numbers.ceilPow2(17));
    }

    @Test
    public void testDoubleCompare() {
        Assert.assertEquals(-1, Numbers.compare(0d, 1d));
        Assert.assertEquals(1, Numbers.compare(1d, 0d));
        Assert.assertEquals(0, Numbers.compare(1d, 1d));
        Assert.assertEquals(0, Numbers.compare(0.0d, 0.0d));
        Assert.assertEquals(0, Numbers.compare(-0.0d, 0.0d));
        Assert.assertEquals(1, Numbers.compare(Double.MAX_VALUE, Double.MIN_VALUE));
        Assert.assertEquals(0, Numbers.compare(Double.MAX_VALUE, Double.MAX_VALUE));
        Assert.assertEquals(0, Numbers.compare(Double.MIN_VALUE, Double.MIN_VALUE));
        Assert.assertEquals(-1, Numbers.compare(Double.MIN_VALUE, Double.MAX_VALUE));
        Assert.assertEquals(0, Numbers.compare(Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY));
        Assert.assertEquals(0, Numbers.compare(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY));
        Assert.assertEquals(0, Numbers.compare(Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY));
        Assert.assertEquals(0, Numbers.compare(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY));
        Assert.assertEquals(0, Numbers.compare(Double.NaN, Double.NaN));
        Assert.assertEquals(-1, Numbers.compare(1d, Double.NaN));
        Assert.assertEquals(-1, Numbers.compare(Double.MIN_VALUE, Double.NaN));
        Assert.assertEquals(-1, Numbers.compare(Double.MAX_VALUE, Double.NaN));
    }

    @Test
    public void testDoubleEquals() {
        Assert.assertTrue(Numbers.equals(1d, 1d));
        Assert.assertTrue(Numbers.equals(0.0d, -0.0d));
        Assert.assertTrue(Numbers.equals(Double.MAX_VALUE, Double.MAX_VALUE));
        Assert.assertTrue(Numbers.equals(Double.MIN_VALUE, Double.MIN_VALUE));
        Assert.assertTrue(Numbers.equals(Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY));
        Assert.assertTrue(Numbers.equals(Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY));
        Assert.assertTrue(Numbers.equals(Double.NaN, Double.NaN));
    }

    @Test(expected = NumericException.class)
    public void testEmptyDouble() throws Exception {
        Numbers.parseDouble("D");
    }

    @Test(expected = NumericException.class)
    public void testEmptyFloat() throws Exception {
        Numbers.parseFloat("f");
    }

    @Test(expected = NumericException.class)
    public void testEmptyLong() throws Exception {
        Numbers.parseLong("L");
    }

    @Test
    public void testEncodeDecodeShortInInt() {
        short[] testCases = new short[]{Short.MIN_VALUE, Short.MAX_VALUE, 0, -1, 1024, -1024, 0xfff, -0xfff};
        for (int i = 0; i < testCases.length; i++) {
            for (int j = 0; j < testCases.length; j++) {
                short hi = testCases[i];
                short lo = testCases[j];
                int encoded = Numbers.encodeLowHighShorts(lo, hi);
                Assert.assertEquals(lo, Numbers.decodeLowShort(encoded));
                Assert.assertEquals(hi, Numbers.decodeHighShort(encoded));
            }
        }
    }

    @Test
    public void testExtractLong256() {
        String invalidInput = "0xogulcan.near";
        String validInputZero = "0x00";
        String validInputMax = "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";
        Long256Impl sink = new Long256Impl();
        Assert.assertFalse(Numbers.extractLong256(invalidInput, sink));
        Assert.assertTrue(Numbers.extractLong256(validInputZero, sink));
        Assert.assertTrue(Numbers.extractLong256(validInputMax, sink));
    }

    @Test
    public void testFloatCompare() {
        Assert.assertEquals(-1, Numbers.compare(0f, 1f));
        Assert.assertEquals(1, Numbers.compare(1f, 0f));
        Assert.assertEquals(0, Numbers.compare(1f, 1f));
        Assert.assertEquals(0, Numbers.compare(0.0f, 0.0f));
        Assert.assertEquals(0, Numbers.compare(-0.0f, 0.0f));
        Assert.assertEquals(1, Numbers.compare(Float.MAX_VALUE, Float.MIN_VALUE));
        Assert.assertEquals(0, Numbers.compare(Float.MAX_VALUE, Float.MAX_VALUE));
        Assert.assertEquals(0, Numbers.compare(Float.MIN_VALUE, Float.MIN_VALUE));
        Assert.assertEquals(-1, Numbers.compare(Float.MIN_VALUE, Float.MAX_VALUE));
        Assert.assertEquals(0, Numbers.compare(Float.POSITIVE_INFINITY, Float.POSITIVE_INFINITY));
        Assert.assertEquals(0, Numbers.compare(Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY));
        Assert.assertEquals(0, Numbers.compare(Float.NEGATIVE_INFINITY, Float.NEGATIVE_INFINITY));
        Assert.assertEquals(0, Numbers.compare(Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY));
        Assert.assertEquals(0, Numbers.compare(Float.NaN, Float.NaN));
    }

    @Test
    public void testFormatByte() {
        for (int i = 0; i < 1000; i++) {
            byte n = (byte) rnd.nextInt();

            sink.clear();
            Numbers.append(sink, n);
            Assert.assertEquals(Byte.toString(n), sink.toString());
        }
    }

    @Test
    public void testFormatChar() {
        for (int i = 0; i < 1000; i++) {
            char n = (char) rnd.nextInt();

            sink.clear();
            Numbers.append(sink, n);
            Assert.assertEquals(Integer.toString(n), sink.toString());
        }
    }

    @Test
    public void testFormatDouble2() {
        sink.clear();
        Numbers.append(sink, 0.8998893432);
        TestUtils.assertEquals("0.8998893432", sink);
    }

    @Test
    public void testFormatDoubleAsRandomFloat() {
        Random random = new Random();
        for (int i = 0; i < 1_000_000; i++) {
            float d1 = random.nextFloat();
            float d2 = (float) random.nextGaussian();
            float d3 = random.nextFloat() * Float.MAX_VALUE;
            sink.clear();
            Numbers.append(sink, (double) d1);
            TestUtils.assertEquals(Double.toString(d1), sink);

            sink.clear();
            Numbers.append(sink, (double) d2);
            TestUtils.assertEquals(Double.toString(d2), sink);

            sink.clear();
            Numbers.append(sink, (double) d3);
            TestUtils.assertEquals(Double.toString(d3), sink);
        }
    }

    @Test
    public void testFormatDoubleExp() {
        sink.clear();
        Numbers.append(sink, 112333.989922222);
        TestUtils.assertEquals("112333.989922222", sink);
    }

    @Test
    public void testFormatDoubleExp10() {
        sink.clear();
        Numbers.append(sink, 1.23E3);
        TestUtils.assertEquals("1230.0", sink);
    }

    @Test
    public void testFormatDoubleExp100() {
        sink.clear();
        Numbers.append(sink, 1.23E105);
        TestUtils.assertEquals("1.23E105", sink);
    }

    @Test
    public void testFormatDoubleExpNeg() {
        sink.clear();
        Numbers.append(sink, -8892.88001);
        TestUtils.assertEquals("-8892.88001", sink);
    }

    @Test
    public void testFormatDoubleFast() {
        sink.clear();
        Numbers.append(sink, -5.9522650387500933e18);
        TestUtils.assertEquals("-5.9522650387500933E18", sink);
    }

    @Test
    public void testFormatDoubleFastInteractive() {
        sink.clear();
        Numbers.append(sink, 0.872989018674569);
        TestUtils.assertEquals("0.872989018674569", sink);
    }

    @Test
    public void testFormatDoubleHugeZero() {
        sink.clear();
        Numbers.append(sink, -0.000000000000001);
        TestUtils.assertEquals("-1.0E-15", sink);
    }

    @Test
    public void testFormatDoubleInt() {
        sink.clear();
        Numbers.append(sink, 44556d);
        TestUtils.assertEquals("44556.0", sink);
    }

    @Test
    public void testFormatDoubleLargeExp() {
        sink.clear();
        Numbers.append(sink, 1123338789079878978979879d);
        TestUtils.assertEquals("1.123338789079879E24", sink);
    }

    @Test
    public void testFormatDoubleNegZero() {
        sink.clear();
        Numbers.append(sink, -0d);
        TestUtils.assertEquals("-0.0", sink);
    }

    @Test
    public void testFormatDoubleNoExponent() {
        sink.clear();
        Numbers.append(sink, 0.2213323334);
        TestUtils.assertEquals("0.2213323334", sink);
    }

    @Test
    public void testFormatDoubleNoExponentNeg() {
        sink.clear();
        Numbers.append(sink, -0.2213323334);
        TestUtils.assertEquals("-0.2213323334", sink);
    }

    @Test
    public void testFormatDoubleRandom() {
        Random random = new Random();
        for (int i = 0; i < 1_000_000; i++) {
            double d1 = random.nextDouble();
            double d2 = random.nextGaussian();
            double d3 = random.nextDouble() * Double.MAX_VALUE;
            sink.clear();
            Numbers.append(sink, d1);
            TestUtils.assertEquals(Double.toString(d1), sink);

            sink.clear();
            Numbers.append(sink, d2);
            TestUtils.assertEquals(Double.toString(d2), sink);

            sink.clear();
            Numbers.append(sink, d3);
            TestUtils.assertEquals(Double.toString(d3), sink);

        }
    }

    @Test
    public void testFormatDoubleRound() {
        sink.clear();
        Numbers.append(sink, 4455630333333333333333334444d);
        TestUtils.assertEquals("4.4556303333333335E27", sink);
    }

    @Test
    public void testFormatDoubleSlowInteractive() {
        sink.clear();
        Numbers.append(sink, 1.1317400099603851e308);
        TestUtils.assertEquals("1.1317400099603851E308", sink);
    }

    @Test
    public void testFormatDoubleZero() {
        sink.clear();
        Numbers.append(sink, 0d);
        TestUtils.assertEquals("0.0", sink);
    }

    @Test
    public void testFormatDoubleZeroExp() {
        sink.clear();
        Numbers.append(sink, -2.225073858507201E-308);
        TestUtils.assertEquals("-2.225073858507201E-308", sink);
    }

    @Test
    public void testFormatFloat() {
        Numbers.append(sink, Float.POSITIVE_INFINITY, 3);
        Assert.assertEquals(Float.toString(Float.POSITIVE_INFINITY), sink.toString());

        sink.clear();
        Numbers.append(sink, Float.NEGATIVE_INFINITY, 3);
        Assert.assertEquals(Float.toString(Float.NEGATIVE_INFINITY), sink.toString());

        sink.clear();
        Numbers.append(sink, Float.NaN, 3);
        Assert.assertEquals(Float.toString(Float.NaN), sink.toString());

        for (int i = 0; i < 1000; i++) {
            int n = rnd.nextPositiveInt() % 10;
            float f = rnd.nextFloat() * (float) Math.pow(10, n);
            sink.clear();
            Numbers.append(sink, f, 8);
            String actual = sink.toString();
            String expected = Float.toString(f);
            Assert.assertEquals(Float.parseFloat(expected), Float.parseFloat(actual), 0.00001);
        }
    }

    @Test
    public void testFormatInt() {
        for (int i = 0; i < 1000; i++) {
            int n = rnd.nextInt();
            sink.clear();
            Numbers.append(sink, n);
            Assert.assertEquals(Integer.toString(n), sink.toString());
        }
    }

    @Test
    public void testFormatLong() {
        for (int i = 0; i < 1000; i++) {
            long n = rnd.nextLong();
            sink.clear();
            Numbers.append(sink, n);
            Assert.assertEquals(Long.toString(n), sink.toString());
        }
    }

    @Test
    public void testFormatShort() {
        for (int i = 0; i < 1000; i++) {
            short n = (short) rnd.nextInt();

            sink.clear();
            Numbers.append(sink, n);
            Assert.assertEquals(Short.toString(n), sink.toString());
        }
    }

    @Test
    public void testFormatSpecialDouble() {
        double d = -1.040218505859375E10d;
        Numbers.append(sink, d);
        Assert.assertEquals(Double.toString(d), sink.toString());

        sink.clear();
        d = -1.040218505859375E-10d;
        Numbers.append(sink, d);
        Assert.assertEquals(Double.toString(d), sink.toString());
    }

    @Test
    public void testGetBroadcastAddress() throws NumericException {
        Assert.assertEquals("12.2.0.0/12.2.255.255", TestUtils.ipv4ToString2(Numbers.getBroadcastAddress("12.2/16")));
    }

    @Test
    public void testGetBroadcastAddress2() throws NumericException {
        Assert.assertEquals("12.2.0.0/12.2.255.255", TestUtils.ipv4ToString2(Numbers.getBroadcastAddress("12.2.10/16")));
    }

    @Test
    public void testGetBroadcastAddress3() throws NumericException {
        Assert.assertEquals("12.2.10.5/12.2.10.5", TestUtils.ipv4ToString2(Numbers.getBroadcastAddress("12.2.10.5")));
    }

    @Test(expected = NumericException.class)
    public void testGetBroadcastAddress4() throws NumericException {
        Numbers.getBroadcastAddress("12.2.10/a");
    }

    @Test(expected = NumericException.class)
    public void testGetBroadcastAddress5() throws NumericException {
        Numbers.getBroadcastAddress("12.2.10/33");
    }

    @Test
    public void testGetIPv4Netmask() {
        Assert.assertEquals("11111111111111111111111100000000", Integer.toBinaryString(Numbers.getIPv4Netmask("12.2.6.8/24")));
        Assert.assertEquals("10000000000000000000000000000000", Integer.toBinaryString(Numbers.getIPv4Netmask("12.2.6.8/1")));
        Assert.assertEquals("11111111111111111111111111111111", Integer.toBinaryString(Numbers.getIPv4Netmask("12.2.6.8/32")));
        Assert.assertEquals("0", Integer.toBinaryString(Numbers.getIPv4Netmask("12.2.6.8/0")));
        Assert.assertEquals("11111111111111111111111111111111", Integer.toBinaryString(Numbers.getIPv4Netmask("12.2.6.8")));

        Assert.assertEquals("1", Integer.toBinaryString(Numbers.getIPv4Netmask("12.2.6.8/")));
        Assert.assertEquals("1", Integer.toBinaryString(Numbers.getIPv4Netmask("12.2.6.8/-1")));
        Assert.assertEquals("1", Integer.toBinaryString(Numbers.getIPv4Netmask("12.2.6.8/33")));
        Assert.assertEquals("1", Integer.toBinaryString(Numbers.getIPv4Netmask("12.2.6.8/1ABC")));
    }

    @Test
    public void testGetIPv4Subnet() throws NumericException {
        Assert.assertEquals("1100000000100000011000001000/11111111111111111111111100000000", toBinaryString(Numbers.getIPv4Subnet("12.2.6.8/24")));
        Assert.assertEquals("1100000000100000011000001000/11111111111111111111111111111110", toBinaryString(Numbers.getIPv4Subnet("12.2.6.8/31")));

        Assert.assertEquals("11111111111111111111111111111111/0", toBinaryString(Numbers.getIPv4Subnet("255.255.255.255/0")));
        Assert.assertEquals("11111111111111111111111111111111/11111111111111111111111111111110", toBinaryString(Numbers.getIPv4Subnet("255.255.255.255/31")));
        Assert.assertEquals("11111111111111111111111111111111/11111111111111111111111111111111", toBinaryString(Numbers.getIPv4Subnet("255.255.255.255/32")));

        assertFails(() -> Numbers.getIPv4Subnet("1"));
        assertFails(() -> Numbers.getIPv4Subnet("0.1"));
        assertFails(() -> toBinaryString(Numbers.getIPv4Subnet("0.1.2")));
        Assert.assertEquals("1000000100000001100000100/11111111111111111111111111111111", toBinaryString(Numbers.getIPv4Subnet("1.2.3.4")));

        Assert.assertEquals("0/0", toBinaryString(Numbers.getIPv4Subnet("0.0/0")));
        Assert.assertEquals("1/0", toBinaryString(Numbers.getIPv4Subnet("0.0.0.1/0")));
        Assert.assertEquals("1/11111111111111111111111111111111", toBinaryString(Numbers.getIPv4Subnet("0.0.0.1/32")));

        Assert.assertEquals("12.2.6.8/255.255.0.0", TestUtils.ipv4ToString2(Numbers.getIPv4Subnet("12.2.6.8/16")));
    }

    @Test
    public void testHexDigitsLong256() {
        Long256Impl long256 = new Long256Impl();

        // null
        long256.setAll(Long.MIN_VALUE, Long.MIN_VALUE, Long.MIN_VALUE, Long.MIN_VALUE);
        sink.clear();
        long256.toSink(sink);
        Assert.assertEquals(sink.length(), Numbers.hexDigitsLong256(long256));

        long256.setAll(0, Long.MIN_VALUE, Long.MIN_VALUE, Long.MIN_VALUE);
        sink.clear();
        long256.toSink(sink);
        Assert.assertEquals(sink.length(), Numbers.hexDigitsLong256(long256));

        long256.setAll(0, 0, Long.MIN_VALUE, Long.MIN_VALUE);
        sink.clear();
        long256.toSink(sink);
        Assert.assertEquals(sink.length(), Numbers.hexDigitsLong256(long256));

        long256.setAll(0, 0, 0, Long.MIN_VALUE);
        sink.clear();
        long256.toSink(sink);
        Assert.assertEquals(sink.length(), Numbers.hexDigitsLong256(long256));

        long256.setAll(Long.MIN_VALUE, 0, Long.MIN_VALUE, Long.MIN_VALUE);
        sink.clear();
        long256.toSink(sink);
        Assert.assertEquals(sink.length(), Numbers.hexDigitsLong256(long256));

        long256.setAll(Long.MIN_VALUE, Long.MIN_VALUE, 0, Long.MIN_VALUE);
        sink.clear();
        long256.toSink(sink);
        Assert.assertEquals(sink.length(), Numbers.hexDigitsLong256(long256));

        long256.setAll(Long.MIN_VALUE, Long.MIN_VALUE, Long.MIN_VALUE, 0);
        sink.clear();
        long256.toSink(sink);
        Assert.assertEquals(sink.length(), Numbers.hexDigitsLong256(long256));

        for (int i = -1; i < 1024; i++) {
            long256.setAll(i, i, i, i);
            sink.clear();
            long256.toSink(sink);
            Assert.assertEquals(sink.length(), Numbers.hexDigitsLong256(long256));
        }

        long256.setAll(Short.MAX_VALUE, Short.MAX_VALUE, Short.MAX_VALUE, Short.MAX_VALUE);
        sink.clear();
        long256.toSink(sink);
        Assert.assertEquals(sink.length(), Numbers.hexDigitsLong256(long256));

        // We used to print all NaNs here
        long256.setAll(Integer.MIN_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE);
        sink.clear();
        long256.toSink(sink);
        Assert.assertEquals(sink.length(), Numbers.hexDigitsLong256(long256));

        long256.setAll(Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE);
        sink.clear();
        long256.toSink(sink);
        Assert.assertEquals(sink.length(), Numbers.hexDigitsLong256(long256));

        long256.setAll(Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE);
        sink.clear();
        long256.toSink(sink);
        Assert.assertEquals(sink.length(), Numbers.hexDigitsLong256(long256));
    }

    @Test
    public void testHexDigitsLong256Fuzz() {
        final int N = 1000;
        Rnd rnd = TestUtils.generateRandom(null);

        Long256Impl long256 = new Long256Impl();

        for (int i = 0; i < N; i++) {
            long256.setAll(rnd.nextLong(), rnd.nextLong(), rnd.nextLong(), rnd.nextLong());
            sink.clear();
            long256.toSink(sink);
            Assert.assertEquals(sink.length(), Numbers.hexDigitsLong256(long256));
        }
    }

    @Test
    public void testHexInt() throws Exception {
        Assert.assertEquals('w', (char) Numbers.parseHexInt("77"));
        Assert.assertEquals(0xf0, Numbers.parseHexInt("F0"));
        Assert.assertEquals(0xac, Numbers.parseHexInt("ac"));
    }

    @Test
    public void testIntEdge() throws Exception {
        Numbers.append(sink, Integer.MAX_VALUE);
        Assert.assertEquals(Integer.MAX_VALUE, Numbers.parseInt(sink));

        sink.clear();

        Numbers.append(sink, Integer.MIN_VALUE);
        Assert.assertEquals(Integer.MIN_VALUE, Numbers.parseIntQuiet(sink));
    }

    @Test
    public void testLong() throws Exception {
        Rnd rnd = new Rnd();
        StringSink sink = new StringSink();
        for (int i = 0; i < 100; i++) {
            long l1 = rnd.nextLong();
            long l2 = rnd.nextLong();
            sink.clear();

            Numbers.append(sink, l1);
            int p = sink.length();
            Numbers.append(sink, l2);
            Assert.assertEquals(l1, Numbers.parseLong(sink, 0, p));
            Assert.assertEquals(l2, Numbers.parseLong(sink, p, sink.length()));
        }
    }

    @Test
    public void testLong256() {
        CharSequence tok = "0x7ee65ec7b6e3bc3a422a8855e9d7bfd29199af5c2aa91ba39c022fa261bdede7";
        Long256Impl long256 = new Long256Impl();
        Long256FromCharSequenceDecoder.decode(tok, 2, tok.length(), long256);
        long256.toSink(sink);
        CharSequence tokLong256 = sink.toString();
        Assert.assertEquals(tok, tokLong256);

        Long256Impl long256a = new Long256Impl();
        Numbers.parseLong256(tok, long256a);
        sink.clear();
        long256a.toSink(sink);
        CharSequence tokLong256a = sink.toString();
        Assert.assertEquals(tok, tokLong256a);

        Assert.assertEquals(tokLong256, tokLong256a);
    }

    @Test
    public void testLongEdge() throws Exception {
        Numbers.append(sink, Long.MAX_VALUE);
        Assert.assertEquals(Long.MAX_VALUE, Numbers.parseLong(sink));

        sink.clear();

        Numbers.append(sink, Long.MIN_VALUE);
        Assert.assertEquals(Long.MIN_VALUE, Numbers.parseLongQuiet(sink));
    }

    @Test
    public void testLongToHex() {
        long value = -8372462554923253491L;
        StringSink sink = new StringSink();
        Numbers.appendHex(sink, value, false);
        TestUtils.assertEquals(Long.toHexString(value), sink);
    }

    @Test
    public void testLongToHex2() {
        long value = 0x5374f5fbcef4819L;
        StringSink sink = new StringSink();
        Numbers.appendHex(sink, value, false);
        TestUtils.assertEquals("0" + Long.toHexString(value), sink);
    }

    @Test
    public void testLongToHex3() {
        long value = 0xbfbca5da8f0645L;
        StringSink sink = new StringSink();
        Numbers.appendHex(sink, value, false);
        TestUtils.assertEquals(Long.toHexString(value), sink);
    }

    @Test
    public void testLongToString() {
        Numbers.append(sink, 6103390276L);
        TestUtils.assertEquals("6103390276", sink);
    }

    @Test
    public void testLongUtf8Sequence() throws Exception {
        Rnd rnd = new Rnd();
        try (DirectUtf8Sink sink = new DirectUtf8Sink(16)) {
            for (int i = 0; i < 100; i++) {
                long l1 = rnd.nextLong();
                long l2 = rnd.nextLong();
                sink.clear();

                Numbers.append(sink, l1);
                int p = sink.size();
                Numbers.append(sink, l2);
                Assert.assertEquals(l1, Numbers.parseLong(sink, 0, p));
                Assert.assertEquals(l2, Numbers.parseLong(sink, p, sink.size()));
            }
        }
    }

    @Test(expected = NumericException.class)
    public void testParse000Greedy0() throws NumericException {
        Numbers.parseInt000Greedy("", 0, 0);
    }

    @Test
    public void testParse000Greedy1() throws NumericException {
        String input = "2";
        long val = Numbers.parseInt000Greedy(input, 0, input.length());
        Assert.assertEquals(input.length(), Numbers.decodeHighInt(val));
        Assert.assertEquals(200, Numbers.decodeLowInt(val));
    }

    @Test
    public void testParse000Greedy2() throws NumericException {
        String input = "06";
        long val = Numbers.parseInt000Greedy(input, 0, input.length());
        Assert.assertEquals(input.length(), Numbers.decodeHighInt(val));
        Assert.assertEquals(60, Numbers.decodeLowInt(val));
    }

    @Test
    public void testParse000Greedy3() throws NumericException {
        String input = "219";
        long val = Numbers.parseInt000Greedy(input, 0, input.length());
        Assert.assertEquals(input.length(), Numbers.decodeHighInt(val));
        Assert.assertEquals(219, Numbers.decodeLowInt(val));
    }

    @Test(expected = NumericException.class)
    public void testParse000Greedy4() throws NumericException {
        Numbers.parseInt000Greedy("1234", 0, 4);
    }

    @Test
    public void testParseDouble() throws Exception {

        String s9 = "0.33458980809808359835083490580348503845E203";
        Assert.assertEquals(Double.parseDouble(s9), Numbers.parseDouble(s9), 0.000000001);

        String s0 = "0.33458980809808359835083490580348503845";
        Assert.assertEquals(Double.parseDouble(s0), Numbers.parseDouble(s0), 0.000000001);


        String s1 = "0.45677888912387699";
        Assert.assertEquals(Double.parseDouble(s1), Numbers.parseDouble(s1), 0.000000001);

        String s2 = "1.459983E35";
        Assert.assertEquals(Double.parseDouble(s2) / 1e35d, Numbers.parseDouble(s2) / 1e35d, 0.00001);


        String s3 = "0.000000023E-30";
        Assert.assertEquals(Double.parseDouble(s3), Numbers.parseDouble(s3), 0.000000001);

        String s4 = "0.000000023E-204";
        Assert.assertEquals(Double.parseDouble(s4), Numbers.parseDouble(s4), 0.000000001);

        String s5 = "0.0000E-204";
        Assert.assertEquals(Double.parseDouble(s5), Numbers.parseDouble(s5), 0.000000001);

        String s6 = "200E2";
        Assert.assertEquals(Double.parseDouble(s6), Numbers.parseDouble(s6), 0.000000001);

        String s7 = "NaN";
        Assert.assertEquals(Double.parseDouble(s7), Numbers.parseDouble(s7), 0.000000001);

        String s8 = "-Infinity";
        Assert.assertEquals(Double.parseDouble(s8), Numbers.parseDouble(s8), 0.000000001);

        String s10 = "2E+2";
        Assert.assertEquals(Double.parseDouble(s10), Numbers.parseDouble(s10), 0.000000001);

        String s11 = "2E+02";
        Assert.assertEquals(Double.parseDouble(s11), Numbers.parseDouble(s11), 0.000000001);

    }

    @Test
    public void testParseDoubleCloseToZero() throws Exception {
        String s1 = "0.123456789";
        Assert.assertEquals(Double.parseDouble(s1), Numbers.parseDouble(s1), 0.000000001);

        String s2 = "0.12345678901234567890123456789E12";
        Assert.assertEquals(Double.parseDouble(s2), Numbers.parseDouble(s2), 0.000000001);
    }

    @Test
    public void testParseDoubleIntegerLargerThanLongMaxValue() throws Exception {
        String s1 = "9223372036854775808";
        Assert.assertEquals(Double.parseDouble(s1), Numbers.parseDouble(s1), 0.000000001);

        String s2 = "9223372036854775808123";
        Assert.assertEquals(Double.parseDouble(s2), Numbers.parseDouble(s2), 0.000000001);

        String s3 = "92233720368547758081239223372036854775808123";
        Assert.assertEquals(Double.parseDouble(s3), Numbers.parseDouble(s3), 0.000000001);

        String s4 = "9223372036854775808123922337203685477580812392233720368547758081239223372036854775808123";
        Assert.assertEquals(Double.parseDouble(s4), Numbers.parseDouble(s4), 0.000000001);
    }

    @Test
    public void testParseDoubleLargerThanLongMaxValue() throws NumericException {
        String s1 = "9223372036854775808.0123456789";
        Assert.assertEquals(Double.parseDouble(s1), Numbers.parseDouble(s1), 0.000000001);

        String s2 = "9223372036854775808.0123456789";
        Assert.assertEquals(Double.parseDouble(s2), Numbers.parseDouble(s2), 0.000000001);

        String s3 = "9223372036854775808123.0123456789";
        Assert.assertEquals(Double.parseDouble(s3), Numbers.parseDouble(s3), 0.000000001);

        String s4 = "92233720368547758081239223372036854775808123.01239223372036854775808123";
        Assert.assertEquals(Double.parseDouble(s4), Numbers.parseDouble(s4), 0.000000001);
    }

    @Test
    public void testParseDoubleNegativeZero() throws NumericException {
        double actual = Numbers.parseDouble("-0.0");

        //check it's zero at all
        Assert.assertEquals(0, actual, 0.0);

        //check it's *negative* zero
        double res = 1 / actual;
        Assert.assertEquals(Double.NEGATIVE_INFINITY, res, 0.0);
    }

    @Test
    public void testParseDoubleWithManyLeadingZeros() throws Exception {
        String s1 = "000000.000000000033458980809808359835083490580348503845";
        Assert.assertEquals(Double.parseDouble(s1), Numbers.parseDouble(s1), 0.000000001);

        String s2 = "000000.00000000003345898080E25";
        Assert.assertEquals(Double.parseDouble(s2), Numbers.parseDouble(s2), 0.000000001);
    }

    @Test
    public void testParseExplicitDouble() throws Exception {
        Assert.assertEquals(1234.123d, Numbers.parseDouble("1234.123d"), 0.000001);
    }

    @Test
    public void testParseExplicitFloat() throws Exception {
        Assert.assertEquals(12345.02f, Numbers.parseFloat("12345.02f"), 0.0001f);
    }

    @Test(expected = NumericException.class)
    public void testParseExplicitFloat2() throws Exception {
        Numbers.parseFloat("12345.02fx");
    }

    @Test
    public void testParseFloat() throws Exception {
        String s1 = "0.45677899234";
        Assert.assertEquals(Float.parseFloat(s1), Numbers.parseFloat(s1), 0.000000001);

        String s2 = "1.459983E35";
        Assert.assertEquals(Float.parseFloat(s2) / 1e35d, Numbers.parseFloat(s2) / 1e35d, 0.00001);

        String s3 = "0.000000023E-30";
        Assert.assertEquals(Float.parseFloat(s3), Numbers.parseFloat(s3), 0.000000001);

        // overflow
        try {
            Numbers.parseFloat("1.0000E-204");
            Assert.fail();
        } catch (NumericException ignored) {
        }

        try {
            Numbers.parseFloat("1E39");
            Assert.fail();
        } catch (NumericException ignored) {
        }

        try {
            Numbers.parseFloat("1.0E39");
            Assert.fail();
        } catch (NumericException ignored) {
        }

        String s6 = "200E2";
        Assert.assertEquals(Float.parseFloat(s6), Numbers.parseFloat(s6), 0.000000001);

        String s7 = "NaN";
        Assert.assertEquals(Float.parseFloat(s7), Numbers.parseFloat(s7), 0.000000001);

        String s8 = "-Infinity";
        Assert.assertEquals(Float.parseFloat(s8), Numbers.parseFloat(s8), 0.000000001);

        // min exponent float
        String s9 = "1.4e-45";
        Assert.assertEquals(1.4e-45f, Numbers.parseFloat(s9), 0.001);

        // false overflow
        String s10 = "0003000.0e-46";
        Assert.assertEquals(1.4e-45f, Numbers.parseFloat(s10), 0.001);

        // false overflow
        String s11 = "0.00001e40";
        Assert.assertEquals(1e35f, Numbers.parseFloat(s11), 0.001);
    }

    @Test
    public void testParseFloatCloseToZero() throws Exception {
        String s1 = "0.123456789";
        Assert.assertEquals(Float.parseFloat(s1), Numbers.parseFloat(s1), 0.000000001);

        String s2 = "0.12345678901234567890123456789E12";
        Assert.assertEquals(Float.parseFloat(s2), Numbers.parseFloat(s2), 0.000000001);
    }

    @Test
    public void testParseFloatIntegerLargerThanLongMaxValue() throws Exception {
        String s1 = "9223372036854775808";
        Assert.assertEquals(Float.parseFloat(s1), Numbers.parseFloat(s1), 0.000000001);

        String s2 = "9223372036854775808123";
        Assert.assertEquals(Float.parseFloat(s2), Numbers.parseFloat(s2), 0.000000001);

        String s3 = "9223372036854775808123922337203685477";
        Assert.assertEquals(Float.parseFloat(s3), Numbers.parseFloat(s3), 0.000000001);

        String s4 = "92233720368547758081239223372036854771";
        Assert.assertEquals(Float.parseFloat(s4), Numbers.parseFloat(s4), 0.000000001);
    }

    @Test
    public void testParseFloatLargerThanLongMaxValue() throws NumericException {
        String s1 = "9223372036854775808.0123456789";
        Assert.assertEquals(Float.parseFloat(s1), Numbers.parseFloat(s1), 0.000000001);

        String s2 = "9223372036854775808.0123456789";
        Assert.assertEquals(Float.parseFloat(s2), Numbers.parseFloat(s2), 0.000000001);

        String s3 = "9223372036854775808123.0123456789";
        Assert.assertEquals(Float.parseFloat(s3), Numbers.parseFloat(s3), 0.000000001);

        String s4 = "922337203685477580812392233720368547758081.01239223372036854775808123"; // overflow
        try {
            Numbers.parseFloat(s4);
            Assert.fail();
        } catch (NumericException ignored) {
        }
    }

    @Test
    public void testParseFloatNegativeZero() throws NumericException {
        float actual = Numbers.parseFloat("-0.0");

        //check it's zero at all
        Assert.assertEquals(0, actual, 0.0);

        //check it's *negative* zero
        float res = 1 / actual;
        Assert.assertEquals(Float.NEGATIVE_INFINITY, res, 0.0);
    }

    @Test
    public void testParseIPv4() throws Exception {
        Assert.assertEquals(84413540, Numbers.parseIPv4("5.8.12.100"));
        Assert.assertEquals(204327201, Numbers.parseIPv4("12.45.201.33"));
    }

    @Test
    public void testParseIPv42() throws Exception {
        Assert.assertEquals(0, Numbers.parseIPv4((CharSequence) null));
        Assert.assertEquals(0, Numbers.parseIPv4("null"));
        Assert.assertEquals(0, Numbers.parseIPv4((Utf8Sequence) null));
        Assert.assertEquals(0, Numbers.parseIPv4(new Utf8String("null")));
    }

    @Test(expected = NumericException.class)
    public void testParseIPv4Empty() throws Exception {
        Numbers.parseIPv4("");
    }

    @Test
    public void testParseIPv4LeadingAndTrailingDots() throws Exception {
        Assert.assertEquals("1.2.3.4", TestUtils.ipv4ToString(Numbers.parseIPv4(".....1.2.3.4......")));
    }

    @Test
    public void testParseIPv4LeadingAndTrailingDots2() throws Exception {
        Assert.assertEquals("1.2.3.4", TestUtils.ipv4ToString(Numbers.parseIPv4(".1.2.3.4.")));
    }

    @Test
    public void testParseIPv4LeadingDots() throws Exception {
        Assert.assertEquals("1.2.3.4", TestUtils.ipv4ToString(Numbers.parseIPv4(".....1.2.3.4")));
    }

    @Test
    public void testParseIPv4LeadingDots2() throws Exception {
        Assert.assertEquals("1.2.3.4", TestUtils.ipv4ToString(Numbers.parseIPv4(".1.2.3.4")));
    }

    @Test(expected = NumericException.class)
    public void testParseIPv4LeadingDots3() throws Exception {
        Numbers.parseIPv4("...a.1.2.3.4");
    }

    @Test(expected = NumericException.class)
    public void testParseIPv4MiddleDots() throws Exception {
        Numbers.parseIPv4("1..2..3..4");
    }

    @Test(expected = NumericException.class)
    public void testParseIPv4Overflow1() throws Exception {
        String i1 = "256.256.256.256";
        Numbers.parseIPv4(i1);
    }

    @Test(expected = NumericException.class)
    public void testParseIPv4Overflow2() throws Exception {
        String i1 = "255.255.255.256";
        Numbers.parseIPv4(i1);
    }

    @Test(expected = NumericException.class)
    public void testParseIPv4Overflow3() throws Exception {
        Numbers.parseIPv4("12.1.3500.2");
    }

    @Test
    public void testParseIPv4Quiet() {
        Assert.assertEquals(0, Numbers.parseIPv4Quiet(null));
        Assert.assertEquals(0, Numbers.parseIPv4Quiet("NaN"));
    }

    @Test(expected = NumericException.class)
    public void testParseIPv4SignOnly() throws Exception {
        Numbers.parseIPv4("-");
    }

    @Test
    public void testParseIPv4TrailingDots() throws Exception {
        Assert.assertEquals("1.2.3.4", TestUtils.ipv4ToString(Numbers.parseIPv4("1.2.3.4......")));
    }

    @Test
    public void testParseIPv4TrailingDots2() throws Exception {
        Assert.assertEquals("1.2.3.4", TestUtils.ipv4ToString(Numbers.parseIPv4("1.2.3.4.")));
    }

    @Test(expected = NumericException.class)
    public void testParseIPv4TrailingDots3() throws Exception {
        Numbers.parseIPv4("1.2.3.4...a.");
    }

    @Test(expected = NumericException.class)
    public void testParseIPv4WrongChars() throws Exception {
        Numbers.parseIPv4("1.2.3.ab");
    }

    @Test
    public void testParseIPv4_0() throws Exception {
        Assert.assertEquals(84413540, Numbers.parseIPv4_0("5.8.12.100", 0, 10));
        Assert.assertEquals(204327201, Numbers.parseIPv4_0("12.45.201.33", 0, 12));
    }

    @Test(expected = NumericException.class)
    public void testParseIPv4_0Empty() throws Exception {
        Numbers.parseIPv4_0("", 0, 0);
    }

    @Test(expected = NumericException.class)
    public void testParseIPv4_0Null() throws Exception {
        Numbers.parseIPv4_0(null, 0, 0);
    }

    @Test(expected = NumericException.class)
    public void testParseIPv4_0Overflow1() throws Exception {
        String i1 = "256.256.256.256";
        Numbers.parseIPv4_0(i1, 0, 15);
    }

    @Test(expected = NumericException.class)
    public void testParseIPv4_0Overflow2() throws Exception {
        String i1 = "255.255.255.256";
        Numbers.parseIPv4_0(i1, 0, 15);
    }

    @Test(expected = NumericException.class)
    public void testParseIPv4_0Overflow3() throws Exception {
        Numbers.parseIPv4_0("12.1.3500.2", 0, 11);
    }

    @Test(expected = NumericException.class)
    public void testParseIPv4_0SignOnly() throws Exception {
        Numbers.parseIPv4_0("-", 0, 1);
    }

    @Test(expected = NumericException.class)
    public void testParseIPv4_0WrongChars() throws Exception {
        Numbers.parseIPv4_0("1.2.3.ab", 0, 8);
    }

    @Test(expected = NumericException.class)
    public void testParseIPv4_0WrongCount() throws Exception {
        Numbers.parseIPv4_0("5.6", 0, 3);
    }

    @Test
    public void testParseInt() throws Exception {
        Assert.assertEquals(567963, Numbers.parseInt("567963"));
        Assert.assertEquals(-23346346, Numbers.parseInt("-23346346"));
        Assert.assertEquals(567963, Numbers.parseInt(new Utf8String("567963")));
        Assert.assertEquals(-23346346, Numbers.parseInt(new Utf8String("-23346346")));
    }

    @Test(expected = NumericException.class)
    public void testParseIntEmpty() throws Exception {
        Numbers.parseInt("");
    }

    @Test(expected = NumericException.class)
    public void testParseIntNull() throws Exception {
        Numbers.parseInt((CharSequence) null);
    }

    @Test(expected = NumericException.class)
    public void testParseIntOverflow1() throws Exception {
        String i1 = "12345566787";
        Numbers.parseInt(i1);
    }

    @Test(expected = NumericException.class)
    public void testParseIntOverflow2() throws Exception {
        Numbers.parseInt("2147483648");
    }

    @Test(expected = NumericException.class)
    public void testParseIntOverflow3() throws Exception {
        Numbers.parseInt("5000000000");
    }

    @Test(expected = NumericException.class)
    public void testParseIntSignOnly() throws Exception {
        Numbers.parseInt("-");
    }

    @Test(expected = NumericException.class)
    public void testParseIntSizeFail() throws Exception {
        Numbers.parseIntSize("5Kb");
    }

    @Test
    public void testParseIntSizeKb() throws Exception {
        Assert.assertEquals(5 * 1024, Numbers.parseIntSize("5K"));
        Assert.assertEquals(5 * 1024, Numbers.parseIntSize("5k"));
    }

    @Test
    public void testParseIntSizeMb() throws Exception {
        Assert.assertEquals(5 * 1024 * 1024, Numbers.parseIntSize("5M"));
        Assert.assertEquals(5 * 1024 * 1024, Numbers.parseIntSize("5m"));
    }

    @Test(expected = NumericException.class)
    public void testParseIntSizeOverflowAtK() throws Exception {
        Numbers.parseIntSize("4194304K");
    }

    @Test(expected = NumericException.class)
    public void testParseIntSizeOverflowAtM() throws Exception {
        Numbers.parseIntSize("10240M");
    }

    @Test(expected = NumericException.class)
    public void testParseIntSizeOverflowNoQualifier() throws Exception {
        Numbers.parseIntSize("10737418240");
    }

    @Test
    public void testParseIntToDelim() throws Exception {
        String in = "1234x5";
        long val = Numbers.parseIntSafely(in, 0, in.length());
        Assert.assertEquals(1234, Numbers.decodeLowInt(val));
        Assert.assertEquals(4, Numbers.decodeHighInt(val));
    }

    @Test(expected = NumericException.class)
    public void testParseIntToDelimEmpty() throws Exception {
        String in = "x";
        Numbers.parseIntSafely(in, 0, in.length());
    }

    @Test
    public void testParseIntToDelimNoChar() throws Exception {
        String in = "12345";
        long val = Numbers.parseIntSafely(in, 0, in.length());
        Assert.assertEquals(12345, Numbers.decodeLowInt(val));
        Assert.assertEquals(5, Numbers.decodeHighInt(val));
    }

    @Test(expected = NumericException.class)
    public void testParseIntWrongChars() throws Exception {
        Numbers.parseInt("123ab");
    }

    @Test
    public void testParseLongDurationDay() throws Exception {
        Assert.assertEquals(20 * Timestamps.DAY_MICROS, Numbers.parseLongDuration("20d"));
    }

    @Test(expected = NumericException.class)
    public void testParseLongDurationFail() throws Exception {
        Numbers.parseLongDuration("5year");
    }

    @Test
    public void testParseLongDurationHour() throws Exception {
        Assert.assertEquals(20 * Timestamps.HOUR_MICROS, Numbers.parseLongDuration("20h"));
    }

    @Test
    public void testParseLongDurationMinute() throws Exception {
        Assert.assertEquals(20 * Timestamps.MINUTE_MICROS, Numbers.parseLongDuration("20m"));
    }

    @Test
    public void testParseLongDurationMonth() throws Exception {
        Assert.assertEquals(20 * 30 * Timestamps.DAY_MICROS, Numbers.parseLongDuration("20M"));
    }

    @Test(expected = NumericException.class)
    public void testParseLongDurationOverflowAtDay() throws Exception {
        Numbers.parseLongDuration("106751992d");
    }

    @Test(expected = NumericException.class)
    public void testParseLongDurationOverflowAtHour() throws Exception {
        Numbers.parseLongDuration("2562047789h");
    }

    @Test(expected = NumericException.class)
    public void testParseLongDurationOverflowAtMinute() throws Exception {
        Numbers.parseLongDuration("153722867281m");
    }

    @Test(expected = NumericException.class)
    public void testParseLongDurationOverflowAtMonth() throws Exception {
        Numbers.parseLongDuration("3558400M");
    }

    @Test(expected = NumericException.class)
    public void testParseLongDurationOverflowAtSecond() throws Exception {
        Numbers.parseLongDuration("9223372036855s");
    }

    @Test(expected = NumericException.class)
    public void testParseLongDurationOverflowAtWeek() throws Exception {
        Numbers.parseLongDuration("15250285w");
    }

    @Test(expected = NumericException.class)
    public void testParseLongDurationOverflowAtYear() throws Exception {
        Numbers.parseLongDuration("292472y");
    }

    @Test(expected = NumericException.class)
    public void testParseLongDurationOverflowNoQualifier() throws Exception {
        Numbers.parseLongDuration("9223372036854775808");
    }

    @Test
    public void testParseLongDurationSecond() throws Exception {
        Assert.assertEquals(20 * Timestamps.SECOND_MICROS, Numbers.parseLongDuration("20s"));
    }

    @Test
    public void testParseLongDurationWeek() throws Exception {
        Assert.assertEquals(20 * Timestamps.WEEK_MICROS, Numbers.parseLongDuration("20w"));
    }

    @Test
    public void testParseLongDurationYear() throws Exception {
        Assert.assertEquals(20 * 365 * Timestamps.DAY_MICROS, Numbers.parseLongDuration("20y"));
    }

    @Test(expected = NumericException.class)
    public void testParseLongEmpty() throws Exception {
        Numbers.parseLong("");
    }

    @Test(expected = NumericException.class)
    public void testParseLongNullCharSequence() throws Exception {
        Numbers.parseLong((CharSequence) null);
    }

    @Test(expected = NumericException.class)
    public void testParseLongNullCharSequence2() throws Exception {
        Numbers.parseLong((CharSequence) null, 0, 10);
    }

    @Test(expected = NumericException.class)
    public void testParseLongNullUtf8Sequence() throws Exception {
        Numbers.parseLong((Utf8Sequence) null);
    }

    @Test(expected = NumericException.class)
    public void testParseLongNullUtf8Sequence2() throws Exception {
        Numbers.parseLong((Utf8Sequence) null, 0, 10);
    }

    @Test(expected = NumericException.class)
    public void testParseLongOverflow1() throws Exception {
        String i1 = "1234556678723234234234234234234";
        Numbers.parseLong(i1);
    }

    @Test(expected = NumericException.class)
    public void testParseLongOverflow2() throws Exception {
        Numbers.parseLong("9223372036854775808");
    }

    @Test(expected = NumericException.class)
    public void testParseLongSignOnly() throws Exception {
        Numbers.parseLong("-");
    }

    @Test(expected = NumericException.class)
    public void testParseLongSizeFail() throws Exception {
        Numbers.parseLongSize("5Kb");
    }

    @Test
    public void testParseLongSizeGb() throws Exception {
        Assert.assertEquals(7 * 1024 * 1024L * 1024L, Numbers.parseLongSize("7G"));
        Assert.assertEquals(7 * 1024 * 1024L * 1024L, Numbers.parseLongSize("7g"));
    }

    @Test
    public void testParseLongSizeKb() throws Exception {
        Assert.assertEquals(5 * 1024L, Numbers.parseLongSize("5K"));
        Assert.assertEquals(5 * 1024L, Numbers.parseLongSize("5k"));
    }

    @Test
    public void testParseLongSizeMb() throws Exception {
        Assert.assertEquals(5 * 1024 * 1024L, Numbers.parseLongSize("5M"));
        Assert.assertEquals(5 * 1024 * 1024L, Numbers.parseLongSize("5m"));
    }

    @Test(expected = NumericException.class)
    public void testParseLongSizeOverflowAtG() throws Exception {
        Numbers.parseLongSize("4503599627370496G");
    }

    @Test(expected = NumericException.class)
    public void testParseLongSizeOverflowAtK() throws Exception {
        Numbers.parseLongSize("45035996273704960000K");
    }

    @Test(expected = NumericException.class)
    public void testParseLongSizeOverflowAtM() throws Exception {
        Numbers.parseLongSize("450359962737049600M");
    }

    @Test(expected = NumericException.class)
    public void testParseLongSizeOverflowNoQualifier() throws Exception {
        Numbers.parseLongSize("45035996273704960000000");
    }

    @Test
    public void testParseLongUnderscore() throws NumericException {
        Assert.assertEquals(123_000, Numbers.parseLong("123_000"));
        Assert.assertEquals(123_343_123, Numbers.parseLong("123_343_123"));
        assertParseLongException("_889");
        assertParseLongException("__8289");
        assertParseLongException("8289_");
        assertParseLongException("8289__");
        assertParseLongException("82__89");
    }

    @Test(expected = NumericException.class)
    public void testParseLongWrongChars() throws Exception {
        Numbers.parseLong("123ab");
    }

    @Test
    public void testParseMicros() throws NumericException {
        Assert.assertEquals(25_000, Numbers.parseMicros("25ms"));
        Assert.assertEquals(25_000, Numbers.parseMicros("25MS"));
        Assert.assertEquals(1_500_000_000L, Numbers.parseMicros("25m"));
        Assert.assertEquals(1_500_000_000L, Numbers.parseMicros("25M"));
        Assert.assertEquals(14_400_000_000L, Numbers.parseMicros("4h"));
        Assert.assertEquals(10_800_000_000L, Numbers.parseMicros("3H"));
        Assert.assertEquals(90_000_000L, Numbers.parseMicros("90s"));
        Assert.assertEquals(560L, Numbers.parseMicros("560us"));
        Assert.assertEquals(5_600L, Numbers.parseMicros("5_600us"));
        Assert.assertEquals(5L, Numbers.parseMicros("5_600ns"));

        assertParseMicrosException(null);
        assertParseMicrosException("");
        // overflow
        assertParseMicrosException("80980982938408234823048028340284820");


        // check similar keywords are not picked up

        // n
        assertParseMicrosException("60nk");
        assertParseMicrosException("3msa");

        // u
        assertParseMicrosException("60uk");
        assertParseMicrosException("3usa");

        // m
        assertParseMicrosException("60mk");
        assertParseMicrosException("3umsa");

        // s
        assertParseMicrosException("3sk");

        // h
        assertParseMicrosException("3hu");

        // arbitrary
        assertParseMicrosException("3k");

        // assert unit without value

        // n
        assertParseMicrosException("ns");
        assertParseMicrosException("-ns");
        assertParseMicrosException("_ns");

        // u
        assertParseMicrosException("us");
        assertParseMicrosException("-us");
        assertParseMicrosException("_us");

        // m
        assertParseMicrosException("ms");
        assertParseMicrosException("-ms");
        assertParseMicrosException("_ms");

        assertParseMicrosException("m");
        assertParseMicrosException("-m");
        assertParseMicrosException("_m");

        // s
        assertParseMicrosException("s");
        assertParseMicrosException("-s");
        assertParseMicrosException("_s");

        // h
        assertParseMicrosException("h");
        assertParseMicrosException("-h");
        assertParseMicrosException("_h");

        // no unit
        assertParseMicrosException("_");

        // underscore misuse
        assertParseMicrosException("_");
        assertParseMicrosException("_22");
        assertParseMicrosException("_444_");
        assertParseMicrosException("_28989__");
        assertParseMicrosException("90902__");
        assertParseMicrosException("123_");
    }

    @Test
    public void testParseMillis() throws NumericException {
        Assert.assertEquals(25, Numbers.parseMillis("25ms"));
        Assert.assertEquals(25, Numbers.parseMillis("25MS"));
        Assert.assertEquals(1_500_000L, Numbers.parseMillis("25m"));
        Assert.assertEquals(1_500_000L, Numbers.parseMillis("25M"));
        Assert.assertEquals(14_400_000L, Numbers.parseMillis("4h"));
        Assert.assertEquals(10_800_000L, Numbers.parseMillis("3H"));
        Assert.assertEquals(90_000L, Numbers.parseMillis("90s"));
        Assert.assertEquals(0L, Numbers.parseMillis("234us"));
        Assert.assertEquals(3_600L, Numbers.parseMillis("3_600_000us"));
        Assert.assertEquals(50L, Numbers.parseMillis("50_600_000ns"));

        assertParseMillisException(null);
        assertParseMillisException("");
        // overflow
        assertParseMillisException("80980982938408234823048028340284820");


        // check similar keywords are not picked up

        // n
        assertParseMillisException("60nk");
        assertParseMillisException("3msa");

        // u
        assertParseMillisException("60uk");
        assertParseMillisException("3usa");

        // m
        assertParseMillisException("60mk");
        assertParseMillisException("3umsa");

        // s
        assertParseMillisException("3sk");

        // h
        assertParseMillisException("3hu");

        // arbitrary
        assertParseMillisException("3k");

        // assert unit without value

        // n
        assertParseMillisException("ns");
        assertParseMillisException("-ns");
        assertParseMillisException("_ns");

        // u
        assertParseMillisException("us");
        assertParseMillisException("-us");
        assertParseMillisException("_us");

        // m
        assertParseMillisException("ms");
        assertParseMillisException("-ms");
        assertParseMillisException("_ms");

        assertParseMillisException("m");
        assertParseMillisException("-m");
        assertParseMillisException("_m");

        // s
        assertParseMillisException("s");
        assertParseMillisException("-s");
        assertParseMillisException("_s");

        // h
        assertParseMillisException("h");
        assertParseMillisException("-h");
        assertParseMillisException("_h");

        // no unit
        assertParseMillisException("_");

        // underscore misuse
        assertParseMillisException("_");
        assertParseMillisException("_22");
        assertParseMillisException("_444_");
        assertParseMillisException("_28989__");
        assertParseMillisException("90902__");
        assertParseMillisException("123_");
    }

    @Test
    public void testParseNanos() throws NumericException {
        Assert.assertEquals(25_000_000, Numbers.parseNanos("25ms"));
        Assert.assertEquals(25_000_000, Numbers.parseNanos("25MS"));
        Assert.assertEquals(1_500_000_000_000L, Numbers.parseNanos("25m"));
        Assert.assertEquals(1_500_000_000_000L, Numbers.parseNanos("25M"));
        Assert.assertEquals(14_400_000_000_000L, Numbers.parseNanos("4h"));
        Assert.assertEquals(10_800_000_000_000L, Numbers.parseNanos("3H"));
        Assert.assertEquals(90_000_000_000L, Numbers.parseNanos("90s"));
        Assert.assertEquals(560_000L, Numbers.parseNanos("560us"));
        Assert.assertEquals(5_600_000L, Numbers.parseNanos("5_600us"));
        Assert.assertEquals(5600L, Numbers.parseNanos("5_600ns"));

        assertParseNanosException(null);
        assertParseNanosException("");
        // overflow
        assertParseNanosException("80980982938408234823048028340284820");

        // check similar keywords are not picked up
        // n
        assertParseNanosException("60nk");
        assertParseNanosException("3msa");

        // u
        assertParseNanosException("60uk");
        assertParseNanosException("3usa");

        // m
        assertParseNanosException("60mk");
        assertParseNanosException("3umsa");

        // s
        assertParseNanosException("3sk");

        // h
        assertParseNanosException("3hu");

        // arbitrary
        assertParseNanosException("3k");

        // assert unit without value

        // n
        assertParseNanosException("ns");
        assertParseNanosException("-ns");
        assertParseNanosException("_ns");

        // u
        assertParseNanosException("us");
        assertParseNanosException("-us");
        assertParseNanosException("_us");

        // m
        assertParseNanosException("ms");
        assertParseNanosException("-ms");
        assertParseNanosException("_ms");

        assertParseNanosException("m");
        assertParseNanosException("-m");
        assertParseNanosException("_m");

        // s
        assertParseNanosException("s");
        assertParseNanosException("-s");
        assertParseNanosException("_s");

        // h
        assertParseNanosException("h");
        assertParseNanosException("-h");
        assertParseNanosException("_h");

        // no unit
        assertParseNanosException("_");

        // underscore misuse
        assertParseNanosException("_");
        assertParseNanosException("_22");
        assertParseNanosException("_444_");
        assertParseNanosException("_28989__");
        assertParseNanosException("90902__");
        assertParseNanosException("123_");
    }

    @Test
    public void testParseSubnet() throws NumericException {
        Assert.assertEquals("12.2.10.0/255.255.255.0", TestUtils.ipv4ToString2(Numbers.parseSubnet("12.2.10/24")));
        Assert.assertEquals("2.4.8.0/255.255.255.0", TestUtils.ipv4ToString2(Numbers.parseSubnet("2.4.8/24")));

        assertFails(() -> Numbers.parseSubnet("2.4.6"));
        assertFails(() -> Numbers.parseSubnet("apple"));
        assertFails(() -> Numbers.parseSubnet("apple/24"));
    }

    @Test(expected = NumericException.class)
    public void testParseSubnet0() throws Exception {
        Numbers.parseSubnet0("apple", 0, 0, 6);
    }

    @Test(expected = NumericException.class)
    public void testParseSubnet01() throws Exception {
        Numbers.parseSubnet0("apple", 0, 5, 6);
    }

    @Test(expected = NumericException.class)
    public void testParseSubnet010() throws Exception {
        Numbers.parseSubnet0("2.2.2.256", 0, 9, 32);
    }

    @Test
    public void testParseSubnet011() throws Exception {
        Assert.assertEquals("2.0.0.0", TestUtils.ipv4ToString(Numbers.parseSubnet0("2", 0, 1, 8)));
    }

    @Test
    public void testParseSubnet012() throws Exception {
        Assert.assertEquals(-2, Numbers.parseSubnet0("255.255.255.254", 0, 15, 31));
    }

    @Test(expected = NumericException.class)
    public void testParseSubnet02() throws Exception {
        Numbers.parseSubnet0("650.650.650.650", 0, 15, 24);
    }

    @Test(expected = NumericException.class)
    public void testParseSubnet03() throws Exception {
        Numbers.parseSubnet0("650.650.650.650.", 0, 15, 24);
    }

    @Test(expected = NumericException.class)
    public void testParseSubnet04() throws Exception {
        Numbers.parseSubnet0("1.2.3.650.", 0, 15, 24);
    }

    @Test(expected = NumericException.class)
    public void testParseSubnet05() throws Exception {
        Numbers.parseSubnet0("2", 0, 1, 16);
    }

    @Test(expected = NumericException.class)
    public void testParseSubnet06() throws Exception {
        Numbers.parseSubnet0("2.2", 0, 3, 24);
    }

    @Test(expected = NumericException.class)
    public void testParseSubnet07() throws Exception {
        Numbers.parseSubnet0("2.2.2", 0, 5, 32);
    }

    @Test(expected = NumericException.class)
    public void testParseSubnet08() throws Exception {
        Numbers.parseSubnet0("2.2.2.2", 0, 7, 33);
    }

    @Test(expected = NumericException.class)
    public void testParseSubnet09() throws Exception {
        Numbers.parseSubnet0("2.2.2.2.", 0, 8, 32);
    }

    @Test
    public void testParseSubnet1() throws Exception {
        Assert.assertEquals("12.2.0.0/255.255.0.0", TestUtils.ipv4ToString2(Numbers.parseSubnet("12.2.10/16")));
    }

    @Test(expected = NumericException.class)
    public void testParseWrongHexInt() throws Exception {
        Numbers.parseHexInt("0N");
    }

    @Test(expected = NumericException.class)
    public void testParseWrongNan() throws Exception {
        Numbers.parseDouble("NaN1");
    }

    @Test
    public void testReverseBits() {
        // this is a simple method to convert BigEndian to readable LittleEndian and vice versa.
        // The test will check if it does the same as the Java library

        int bufSize = 4096;
        int count = bufSize / Integer.BYTES;
        ByteBuffer buf = ByteBuffer.allocate(bufSize);
        buf.order(ByteOrder.LITTLE_ENDIAN);
        Rnd rnd = TestUtils.generateRandom(null);
        IntList values = new IntList(count);
        for (int i = 0; i < count; i++) {
            int val = rnd.nextInt();
            buf.putInt(val);
            values.add(val);
        }
        buf.rewind();
        buf.order(ByteOrder.BIG_ENDIAN);

        for (int i = 0; i < count; i++) {
            int val = values.get(i);
            Assert.assertEquals(buf.getInt(), Numbers.reverseBits(val));
        }
    }

    @Test
    public void testRoundDown() throws Exception {
        Rnd rnd = new Rnd();
        for (int i = 0; i < 1000; i++) {
            double d = rnd.nextDouble();
            double n = Numbers.roundDown(d, 8);
            Assert.assertTrue(d + " " + n + " " + (d - n - 1E-8), d - n - 1E-8 < Numbers.TOLERANCE);
        }
    }

    @Test
    public void testRoundHalfDown() throws Exception {
        Assert.assertEquals(-1.235, Numbers.roundHalfDown(-1.2346, 3), Numbers.TOLERANCE);
        Assert.assertEquals(-1.23489, Numbers.roundHalfDown(-1.234895, 5), Numbers.TOLERANCE);
        Assert.assertEquals(1.23489, Numbers.roundHalfDown(1.234895, 5), Numbers.TOLERANCE);
    }

    @Test
    public void testRoundHalfEven() throws Exception {
        Assert.assertEquals(-1.235, Numbers.roundHalfEven(-1.2346, 3), Numbers.TOLERANCE);
        Assert.assertEquals(-1.2349, Numbers.roundHalfEven(-1.234899, 5), Numbers.TOLERANCE);
        Assert.assertEquals(1.2349, Numbers.roundHalfEven(1.234899, 5), Numbers.TOLERANCE);
        Assert.assertEquals(1.2349, Numbers.roundHalfEven(1.2348995, 6), Numbers.TOLERANCE);

        Assert.assertEquals(-1.2349, Numbers.roundHalfEven(-1.234895, 5), Numbers.TOLERANCE);
        Assert.assertEquals(1.2349, Numbers.roundHalfEven(1.234895, 5), Numbers.TOLERANCE);
        Assert.assertEquals(1.0008, Numbers.roundHalfEven(1.00075, 4), Numbers.TOLERANCE);
        Assert.assertEquals(1.0008, Numbers.roundHalfEven(1.00085, 4), Numbers.TOLERANCE);
        Assert.assertEquals(24, Numbers.roundHalfEven(23.5, 0), Numbers.TOLERANCE);
        Assert.assertEquals(24, Numbers.roundHalfEven(24.5, 0), Numbers.TOLERANCE);
        Assert.assertEquals(-24, Numbers.roundHalfEven(-23.5, 0), Numbers.TOLERANCE);
        Assert.assertEquals(-24, Numbers.roundHalfEven(-24.5, 0), Numbers.TOLERANCE);
    }

    @Test
    public void testRoundHalfUp() throws Exception {
        Assert.assertEquals(-1.235, Numbers.roundHalfUp(-1.2346, 3), Numbers.TOLERANCE);
        Assert.assertEquals(-1.2349, Numbers.roundHalfUp(-1.234899, 5), Numbers.TOLERANCE);
        Assert.assertEquals(1.2349, Numbers.roundHalfUp(1.234895, 5), Numbers.TOLERANCE);
        Assert.assertEquals(1.0009, Numbers.roundHalfUp(1.00091, 4), Numbers.TOLERANCE);
        Assert.assertEquals(-1.0009, Numbers.roundHalfUp(-1.00091, 4), Numbers.TOLERANCE);
    }

    @Test
    public void testRoundUp() throws Exception {
        Assert.assertEquals(-0.2345678098023, Numbers.roundUp(-0.234567809802242442424242423122388, 13), 1E-14);
        Assert.assertEquals(0.2345678098023, Numbers.roundUp(0.234567809802242442424242423122388, 13), 1E-14);

        Rnd rnd = new Rnd();
        for (int i = 0; i < 1000; i++) {
            double d = rnd.nextDouble();
            double n = Numbers.roundUp(d, 8);
            Assert.assertTrue(d + " " + n + " " + (n - d - 1E-8), n - d - 1E-8 < Numbers.TOLERANCE);
        }
    }

    @Test
    public void testShortBswap() {
        short v = Numbers.bswap((short) -7976);
        Assert.assertEquals(-7976, Numbers.bswap(v));
    }

    @Test
    public void testSinkSizeInt() throws NumericException {
        int ipv4 = Numbers.IPv4_NULL;
        sink.clear();
        Numbers.intToIPv4Sink(sink, ipv4);
        Assert.assertEquals(sink.length(), Numbers.sinkSizeIPv4(ipv4));

        ipv4 = Numbers.parseIPv4("0.0.0.1");
        sink.clear();
        Numbers.intToIPv4Sink(sink, ipv4);
        Assert.assertEquals(sink.length(), Numbers.sinkSizeIPv4(ipv4));

        ipv4 = Numbers.parseIPv4("0.0.1.1");
        sink.clear();
        Numbers.intToIPv4Sink(sink, ipv4);
        Assert.assertEquals(sink.length(), Numbers.sinkSizeIPv4(ipv4));

        ipv4 = Numbers.parseIPv4("0.1.1.1");
        sink.clear();
        Numbers.intToIPv4Sink(sink, ipv4);
        Assert.assertEquals(sink.length(), Numbers.sinkSizeIPv4(ipv4));

        ipv4 = Numbers.parseIPv4("1.1.1.1");
        sink.clear();
        Numbers.intToIPv4Sink(sink, ipv4);
        Assert.assertEquals(sink.length(), Numbers.sinkSizeIPv4(ipv4));

        ipv4 = Numbers.parseIPv4("255.255.255.255");
        sink.clear();
        Numbers.intToIPv4Sink(sink, ipv4);
        Assert.assertEquals(sink.length(), Numbers.sinkSizeIPv4(ipv4));

        ipv4 = Numbers.parseIPv4("128.0.0.1");
        sink.clear();
        Numbers.intToIPv4Sink(sink, ipv4);
        Assert.assertEquals(sink.length(), Numbers.sinkSizeIPv4(ipv4));
    }

    private static void assertFails(ExceptionalRunnable r) {
        try {
            r.run();
            Assert.fail("Exception of class " + NumericException.class + " expected!");
        } catch (Exception t) {
            Assert.assertEquals(NumericException.class, t.getClass());
        }
    }

    private static void assertParseLongException(String input) {
        try {
            Numbers.parseLong(input);
            Assert.fail();
        } catch (NumericException ignore) {
        }
    }

    private static void assertParseMicrosException(String sequence) {
        try {
            Numbers.parseMicros(sequence);
            Assert.fail();
        } catch (NumericException ignore) {
        }
    }

    private static void assertParseMillisException(String sequence) {
        try {
            Numbers.parseMillis(sequence);
            Assert.fail();
        } catch (NumericException ignore) {
        }
    }

    private static void assertParseNanosException(String sequence) {
        try {
            Numbers.parseNanos(sequence);
            Assert.fail();
        } catch (NumericException ignore) {
        }
    }

    private static String toBinaryString(long l) {
        return Integer.toBinaryString((int) (l >> 32)) + "/" + Integer.toBinaryString((int) (l));
    }

    @FunctionalInterface
    interface ExceptionalRunnable {
        void run() throws Exception;
    }
}
