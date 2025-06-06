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

package io.questdb.griffin.engine.functions.columns;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.engine.functions.GeoByteFunction;
import org.jetbrains.annotations.TestOnly;

import static io.questdb.griffin.engine.functions.columns.ColumnUtils.STATIC_COLUMN_COUNT;

public class GeoByteColumn extends GeoByteFunction {
    private static final GeoByteColumn[] COLUMNS;

    private final int columnIndex;

    private GeoByteColumn(int columnIndex, int columnType) {
        super(columnType);
        this.columnIndex = columnIndex;
    }

    public static GeoByteColumn newInstance(int columnIndex, int columnType) {
        assert ColumnType.getGeoHashBits(columnType) >= ColumnType.GEOBYTE_MIN_BITS &&
                ColumnType.getGeoHashBits(columnType) <= ColumnType.GEOBYTE_MAX_BITS;

        final int bits = (ColumnType.GEOBYTE_MAX_BITS - ColumnType.GEOBYTE_MIN_BITS + 1);

        if (columnIndex < STATIC_COLUMN_COUNT) {
            return COLUMNS[columnIndex * bits + ColumnType.getGeoHashBits(columnType) - ColumnType.GEOBYTE_MIN_BITS];
        }

        return new GeoByteColumn(columnIndex, columnType);
    }

    @TestOnly
    public int getColumnIndex() {
        return columnIndex;
    }

    @Override
    public byte getGeoByte(Record rec) {
        return rec.getGeoByte(columnIndex);
    }

    @Override
    public boolean isThreadSafe() {
        return true;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.putColumnName(columnIndex);
    }

    static {
        int bits = ColumnType.GEOBYTE_MAX_BITS - ColumnType.GEOBYTE_MIN_BITS + 1;
        COLUMNS = new GeoByteColumn[STATIC_COLUMN_COUNT * bits];

        for (int col = 0; col < STATIC_COLUMN_COUNT; col++) {
            for (int bit = ColumnType.GEOBYTE_MIN_BITS; bit <= ColumnType.GEOBYTE_MAX_BITS; bit++) {
                COLUMNS[col * bits + bit - ColumnType.GEOBYTE_MIN_BITS] = new GeoByteColumn(col, ColumnType.getGeoHashTypeWithBits(bit));
            }
        }
    }

}
