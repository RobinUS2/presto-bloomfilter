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
        // Test empty string
        assertQuery("WITH a AS (SELECT bloom_filter('') AS bf) SELECT bloom_filter_contains(a.bf, 'test') FROM a", "SELECT false");

        // Test null
        assertQuery("WITH a AS (SELECT bloom_filter(null) AS bf) SELECT bloom_filter_contains(a.bf, 'test') FROM a", "SELECT false");

        // Test positive in bloom filter
        assertQueryTrue("WITH a AS (SELECT bloom_filter('test') AS bf) SELECT bloom_filter_contains(a.bf, 'test') FROM a LIMIT 1");

        // Test negative in filter
        assertQuery("WITH a AS (SELECT bloom_filter('test') AS bf) SELECT bloom_filter_contains(a.bf, 'not-in-here') FROM a LIMIT 1", "SELECT false");

        // Test with config (expected insertions)
        assertQueryTrue("WITH a AS (SELECT bloom_filter('test', 10) AS bf) SELECT bloom_filter_contains(a.bf, 'test') FROM a LIMIT 1");

        // Test with config (expected insertions)
        assertQuery("SELECT get_expected_insertions(bloom_filter('test', 10))", "SELECT 10");

        // Test with config (expected insertions)
        assertQuery("SELECT get_false_positive_percentage(bloom_filter('test', 10, 0.1234))", "SELECT 0.1234");

        // Test with config (expected insertions AND false positive percentage)
        assertQueryTrue("WITH a AS (SELECT bloom_filter('test', 10, 0.001) AS bf) SELECT bloom_filter_contains(a.bf, 'test') FROM a LIMIT 1");

        // Use 2 bloom filters in a single query
        assertQuery("WITH a AS (SELECT bloom_filter('a') AS bf), b AS (SELECT bloom_filter('b') AS bf) SELECT bloom_filter_contains(a.bf, 'a'), bloom_filter_contains(a.bf, 'b'), bloom_filter_contains(b.bf, 'a'), bloom_filter_contains(b.bf, 'b') FROM a,b LIMIT 1", "SELECT true, false, false, true");

        // Test streaming load
        assertQuery("WITH input AS (select 'a' AS uuid union select 'b' AS uuid union select 'c' AS uuid union select 'd' AS uuid), a AS (SELECT bloom_filter(input.uuid) AS bf FROM input) SELECT bloom_filter_contains(a.bf, 'a'), bloom_filter_contains(a.bf, 'not-in-the-list') FROM a LIMIT 1", "SELECT true, false");

        // Test streaming load custom config
        assertQuery("WITH input AS (select 'a' AS uuid union select 'b' AS uuid union select 'c' AS uuid union select 'd' AS uuid), a AS (SELECT bloom_filter(input.uuid, 10) AS bf FROM input) SELECT bloom_filter_contains(a.bf, 'a'), bloom_filter_contains(a.bf, 'not-in-the-list') FROM a LIMIT 1", "SELECT true, false");
    }

    @Test
    public void testBloomFiltersTypeConversions()
            throws Exception
    {
        // Test positive in bloom filter
        assertQuery("SELECT to_string(bloom_filter('', 10))", "SELECT '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'");
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
