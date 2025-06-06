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

package io.questdb.griffin.engine.functions.eq;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.constants.BooleanConstant;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;

public class EqDoubleArrayFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "=(D[]D[])";
    }

    @Override
    public boolean isBoolean() {
        return true;
    }

    @Override
    public Function newInstance(
            int position,
            @Transient ObjList<Function> args,
            @Transient IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        Function left = args.getQuick(0);
        Function right = args.getQuick(1);
        int leftNDims = ColumnType.decodeArrayDimensionality(left.getType());
        int rightNDims = ColumnType.decodeArrayDimensionality(right.getType());
        if (leftNDims == rightNDims) {
            return new DoubleArrayEqualsFunction(left, right);
        }
        left.close();
        right.close();
        return BooleanConstant.FALSE;
    }

    private static class DoubleArrayEqualsFunction extends AbstractEqBinaryFunction implements BinaryFunction {

        public DoubleArrayEqualsFunction(Function left, Function right) {
            super(left, right);
        }

        @Override
        public boolean getBool(Record rec) {
            return negated != left.getArray(rec).arrayEquals(right.getArray(rec));
        }

        @Override
        public Function getLeft() {
            return left;
        }

        @Override
        public final String getName() {
            if (negated) {
                return "!=";
            } else {
                return "=";
            }
        }

        @Override
        public Function getRight() {
            return right;
        }
    }
}
