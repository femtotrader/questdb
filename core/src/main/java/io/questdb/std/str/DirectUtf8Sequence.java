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

package io.questdb.std.str;

import io.questdb.std.Unsafe;
import io.questdb.std.bytes.DirectByteSequence;

/**
 * A sequence of UTF-8 bytes stored in native memory.
 */
public interface DirectUtf8Sequence extends Utf8Sequence, DirectByteSequence {

    @Override
    default byte byteAt(int index) {
        return Unsafe.getUnsafe().getByte(ptr() + index);
    }

    @Override
    default int intAt(int offset) {
        return Unsafe.getUnsafe().getInt(ptr() + offset);
    }

    @Override
    default long longAt(int offset) {
        return Unsafe.getUnsafe().getLong(ptr() + offset);
    }

    @Override
    long ptr();

    @Override
    default short shortAt(int offset) {
        return Unsafe.getUnsafe().getShort(ptr() + offset);
    }
}
