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

package io.questdb.griffin.engine.functions.bool;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.FunctionExtension;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.constants.BooleanConstant;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Utf8SequenceHashSet;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8StringSink;

public class AllNotEqVarcharFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        // even though this is as VARCHAR function, the second argument is still a string array
        return "<>all(Øw)";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        FunctionExtension arrayFunction = args.getQuick(1).extendedOps();
        int arraySize = arrayFunction.getArrayLength();
        if (arraySize == 0) {
            return BooleanConstant.TRUE;
        }

        Utf8SequenceHashSet set = new Utf8SequenceHashSet();
        Utf8StringSink sink = Misc.getThreadLocalUtf8Sink();
        for (int i = 0; i < arraySize; i++) {
            sink.clear();
            sink.put(arrayFunction.getStrA(null, i));
            set.add(sink);
        }

        Function var = args.getQuick(0);
        if (var.isConstant()) {
            final Utf8Sequence str = var.getVarcharA(null);
            return BooleanConstant.of(str != null && set.excludes(str));
        }

        return new AllNotEqualVarcharFunction(var, set);
    }

    private static class AllNotEqualVarcharFunction extends BooleanFunction implements UnaryFunction {
        private final Function arg;
        private final Utf8SequenceHashSet set;

        private AllNotEqualVarcharFunction(Function arg, Utf8SequenceHashSet set) {
            this.arg = arg;
            this.set = set;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public boolean getBool(Record rec) {
            final Utf8Sequence str = arg.getVarcharA(rec);
            return str != null && set.excludes(str);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(arg).val(" <> all ").val(set);
        }
    }
}
