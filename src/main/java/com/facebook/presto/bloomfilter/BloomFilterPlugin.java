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

import javax.inject.Inject;

import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

public class BloomFilterPlugin
        implements Plugin
{

    @Inject
    public void setTypeManager(TypeManager typeManager)
    {
        checkNotNull(typeManager, "typeManager is null");
    }

    @Override
    public Set<Class<?>> getFunctions()
    {
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
        return ImmutableList.of(new BloomFilterParametricType());
    }

    @Override
    public Iterable<Type> getTypes()
    {
        return ImmutableList.of(BloomFilterType.BLOOM_FILTER);
    }
}
