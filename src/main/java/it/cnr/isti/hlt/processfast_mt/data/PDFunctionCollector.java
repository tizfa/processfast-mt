
/*
 *
 * ****************
 * Copyright 2015 Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 *
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
 * ******************
 */

package it.cnr.isti.hlt.processfast_mt.data;

import it.cnr.isti.hlt.processfast.core.TaskDataContext;
import it.cnr.isti.hlt.processfast.data.PDFunction2;
import it.cnr.isti.hlt.processfast.utils.Pair;

import java.util.EnumSet;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

public class PDFunctionCollector<K, V> implements Collector<Pair<K, V>, PDFunctionCollector.InternalAccumulator<V>, V> {
    private PDFunction2<V, V, V> func;
    private TaskDataContext tdc;

    public PDFunctionCollector(TaskDataContext tdc, PDFunction2<V, V, V> func) {
        if (tdc == null) {
            throw new NullPointerException("The task data context is \'null\'");
        } else if (func == null) {
            throw new NullPointerException("The func object is \'null\'");
        } else {
            this.tdc = tdc;
            this.func = func;
        }
    }

    public Supplier<PDFunctionCollector.InternalAccumulator<V>> supplier() {
        return () -> {
            return new PDFunctionCollector.InternalAccumulator<V>();
        };
    }

    public BiConsumer<PDFunctionCollector.InternalAccumulator<V>, Pair<K, V>> accumulator() {
        return (accum, entry) -> {
            if (accum.state == null) {
                accum.state = entry.getV2();
            } else {
                accum.state = this.func.call(this.tdc, accum.state, entry.getV2());
            }

        };
    }

    public BinaryOperator<PDFunctionCollector.InternalAccumulator<V>> combiner() {
        return (x, y) -> {
            x.state = this.func.call(this.tdc, x.state, y.state);
            return x;
        };
    }

    public Function<PDFunctionCollector.InternalAccumulator<V>, V> finisher() {
        return (x) -> {
            return x.state;
        };
    }

    public Set<Characteristics> characteristics() {
        return EnumSet.of(Characteristics.UNORDERED);
    }

    public static class InternalAccumulator<T1> {
        public T1 state;

        public InternalAccumulator() {
        }
    }
}
