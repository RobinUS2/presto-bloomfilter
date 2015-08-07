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

import com.facebook.presto.Session;
import com.facebook.presto.metadata.FunctionFactory;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.facebook.presto.type.ParametricType;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.testng.annotations.Test;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;

public class TestBloomFilterQueries
        extends AbstractTestQueryFramework
{
    public TestBloomFilterQueries()
    {
        super(createLocalQueryRunner());
    }

    @Test
    public void testBloomFilters()
            throws Exception
    {
        // Test positive in bloom filter
        assertQueryTrue("WITH a AS (SELECT bloom_filter('test') AS bf) SELECT bloom_filter_contains(a.bf, 'test') FROM a LIMIT 1");

        // Test negative in filter
        assertQuery("WITH a AS (SELECT bloom_filter('test') AS bf) SELECT bloom_filter_contains(a.bf, 'not-in-here') FROM a LIMIT 1", "SELECT false");

        // Test with config (expected insertions)
        assertQueryTrue("WITH a AS (SELECT bloom_filter('test', 10) AS bf) SELECT bloom_filter_contains(a.bf, 'test') FROM a LIMIT 1");

        // Test with config (expected insertions AND false positive percentage)
        assertQueryTrue("WITH a AS (SELECT bloom_filter('test', 10, 0.001) AS bf) SELECT bloom_filter_contains(a.bf, 'test') FROM a LIMIT 1");

        // Use 2 bloom filters in a single query
        assertQuery("WITH a AS (SELECT bloom_filter('a') AS bf), b AS (SELECT bloom_filter('b') AS bf) SELECT bloom_filter_contains(a.bf, 'a'), bloom_filter_contains(a.bf, 'b'), bloom_filter_contains(b.bf, 'a'), bloom_filter_contains(b.bf, 'b') FROM a,b LIMIT 1", "SELECT true, false, false, true");

        // Test streaming load
        assertQuery("WITH input AS (select 'a' AS uuid union select 'b' AS uuid union select 'c' AS uuid union select 'd' AS uuid), a AS (SELECT bloom_filter(input.uuid) AS bf FROM input) SELECT bloom_filter_contains(a.bf, 'a'), bloom_filter_contains(a.bf, 'not-in-the-list') FROM a LIMIT 1", "SELECT true, false");
    }

    @Test
    public void testBloomFiltersTypeConversions()
            throws Exception
    {
        // Test positive in bloom filter
        assertQuery("SELECT to_string(bloom_filter('', 10))", "SELECT 'p9uesW3uj5I6NZ4K/ewi7VuBHfI89lDUiMNb6lkNZ5sWAAAAAQcAAAACAAAAAAAAAAAAAAAAAAAAAA=='");
    }

    private static LocalQueryRunner createLocalQueryRunner()
    {
        Session defaultSession = testSessionBuilder()
                .setCatalog("local")
                .setSchema(TINY_SCHEMA_NAME)
                .build();

        LocalQueryRunner localQueryRunner = new LocalQueryRunner(defaultSession);

        // add the tpch catalog
        // local queries run directly against the generator
        localQueryRunner.createCatalog(
                defaultSession.getCatalog(),
                new TpchConnectorFactory(localQueryRunner.getNodeManager(), 1),
                ImmutableMap.<String, String>of());

        BloomFilterPlugin plugin = new BloomFilterPlugin();
        plugin.setTypeManager(localQueryRunner.getTypeManager());
        for (Type type : plugin.getServices(Type.class)) {
            localQueryRunner.getTypeManager().addType(type);
        }
        for (ParametricType parametricType : plugin.getServices(ParametricType.class)) {
            localQueryRunner.getTypeManager().addParametricType(parametricType);
        }
        localQueryRunner.getMetadata().getFunctionRegistry().addFunctions(Iterables.getOnlyElement(plugin.getServices(FunctionFactory.class)).listFunctions());

        return localQueryRunner;
    }
}
