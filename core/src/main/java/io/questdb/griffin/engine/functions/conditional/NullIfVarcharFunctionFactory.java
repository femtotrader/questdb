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

package io.questdb.griffin.engine.functions.conditional;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.VarcharFunction;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8s;

public class NullIfVarcharFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "nullif(ØØ)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) {
        return new Func(args.getQuick(0), args.getQuick(1));
    }

    private static class Func extends VarcharFunction implements BinaryFunction {
        private final Function varcharFunc1;
        private final Function varcharFunc2;

        public Func(Function varcharFunc1, Function varcharFunc2) {
            this.varcharFunc1 = varcharFunc1;
            this.varcharFunc2 = varcharFunc2;
        }

        @Override
        public Function getLeft() {
            return varcharFunc1;
        }

        @Override
        public String getName() {
            return "nullif";
        }

        @Override
        public Function getRight() {
            return varcharFunc2;
        }

        @Override
        public Utf8Sequence getVarcharA(Record rec) {
            Utf8Sequence us1 = varcharFunc1.getVarcharA(rec);
            if (us1 == null) {
                return null;
            }
            Utf8Sequence us2 = varcharFunc2.getVarcharA(rec);
            if (us2 == null || !Utf8s.equals(us1, us2)) {
                return us1;
            }
            return null;
        }

        @Override
        public Utf8Sequence getVarcharB(Record rec) {
            Utf8Sequence us1 = varcharFunc1.getVarcharB(rec);
            if (us1 == null) {
                return null;
            }
            Utf8Sequence us2 = varcharFunc2.getVarcharB(rec);
            if (us2 == null || !Utf8s.equals(us1, us2)) {
                return us1;
            }
            return null;
        }
    }
}
