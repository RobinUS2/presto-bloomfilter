/*
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
 */
package com.facebook.presto.bloomfilter;

import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.type.ParametricType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;

import javax.inject.Inject;

import java.util.Set;

public class BloomFilterPlugin
        implements Plugin
{
    private static final Logger log = Logger.get(BloomFilterPlugin.class);

    @Inject
    public void setTypeManager(TypeManager typeManager)
    {
        if (typeManager == null) {
            throw new NullPointerException("typeManager is null");
        }
        log.info("Received type manager");
    }

    @Override
    public Set<Class<?>> getFunctions()
    {
        log.info("Returning bloomfilter functions");
        return ImmutableSet.<Class<?>>builder()
                .add(BloomFilterContainsScalarFunction.class)
                .add(BloomFilterPersistScalarFunction.class)
                .add(BloomFilterToStringScalarFunction.class)
                .add(BloomFilterGetExpectedInsertionsScalarFunction.class)
                .add(BloomFilterGetFalsePositivePercentageScalarFunction.class)
                .add(BloomFilterAggregation.class)
                .add(BloomFilterFromString.class)
                .add(BloomFilterLoad.class)
                .build();
    }

    @Override
    public Iterable<ParametricType> getParametricTypes()
    {
        log.info("Returning bloomfilter parametric type");
        return ImmutableList.of(new BloomFilterParametricType());
    }

    @Override
    public Iterable<Type> getTypes()
    {
        log.info("Returning bloomfilter type");
        return ImmutableList.of(BloomFilterType.BLOOM_FILTER);
    }
}
