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

package io.questdb.test.cairo;

import io.questdb.cairo.CairoError;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

public class CairoErrorTest {
    @Test
    public void testSinkable() {
        var ce = new CairoError("Test error");
        var sink = new StringSink();
        ce.toSink(sink);
        TestUtils.assertEquals("Test error", sink);
    }

    @Test
    public void testSinkableWithCause() {
        var ce = new CairoError(new AssertionError());
        var sink = new StringSink();
        ce.toSink(sink);
        TestUtils.assertEquals("java.lang.AssertionError", sink);
    }

    @Test
    public void testSinkableWithCauseWithMessage() {
        var ce = new CairoError(new AssertionError("massive fail"));
        var sink = new StringSink();
        ce.toSink(sink);
        TestUtils.assertEquals("java.lang.AssertionError: massive fail", sink);
    }
}
