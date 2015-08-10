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
        assertQuery("SELECT to_string(bloom_filter('', 10))", "SELECT 'E3C+FS5Mv7a7AI6kofHMOSY4o3Pqw9v952LC1o6IQGFjAwAACgAAAHsUrkfheoQ/H4sIAAAAAAAAAJVUzWtVRxQ/eXlJmxhj8pRiFwG/Fgbje4tKqWShffkwL76kgbiQvE3n3TvvvUnmztzOnPt8SSFUMG1BBMGPXWkp7pQWkaK4dNNFQf0TUrel4M6FCM7MvTe5KQlt72LucOZ3fnPO75wzD/6GHq1gTCqqkepinUsZNBhHqooBDaRaLZatadqZ5pzl519vlJ/Nbr7OQa4KPc4DoVBdJm1SipDxUpnhIsXxKvR6UjRYE+F4NbmglLmgFJOWI8Z9qsY7oQlkyLIULUsxZhHvvl8/e+b+0W7oqkG+zlAj5GqznTBS7n8k/1vhYXQrB9AJAcD8d34dQ3p0t+x2XP66u/f3P34ov8jBgSXI+1LQJRiWbaquKIa00pjqMI16CfYp6jNdJt4K9asw4MlIIBPNsgvrYCwBJ6JZqgikTZNUFYZoJ6QeUn+K04AK1F/BOvRX4XCDcE0XpGbI2nRByTqpM85wdUtLRzQpozqnhmegRXRrOhIeMikQSrsqOmMwhqrNTE4nZjIOhqDfEsxRbEkf4fR/c4/htpLWmaax5wUJ6M44F1EZIWy+TqIJKQR1N6c+fc4+IzVaw2BqWJAKU1bN1miy79WctE2AcCDTV7apOmEXgKnpsGsUe3Ux0brw6qd7b65+91kOuirQ0yY8op20oRxuPgrqVH374M7Ivtub19OOGXIBDptdv94Bj5X/5tGz2dGVt3nDOpmyWofCuWtrf1745dDXtr+2nZhoyxVaXKSKEW7S8askqPtEkpe3x0Y/3chBfwUGWRDyWNmLTPg100ckxMjI8blq2j6qZXT9or5sdDS6DsYgI/IEJ9rAhjMoZxq3TZXUm3CrimoQj7qzRPORXc7jSOZNRRPQsT1Bi6wpiI00LaHNJMufSS1DeHDb+k+Gj5jQSMwQETMhMeTSahifuvL0mjlPBIkVTgS5+fzyj0N6lKdlhLaCU7vN+R49nb4PBeOvYHCbf0pEQfYwNMPyv0YNIW+HBeHDk7VypTJaqyAc+TcGhP3WacJISYSviBXgmFvW7TqC8PFcpIJIfXKRKe215hiuUREQr0XVVgN/gAB2fwihj0uP8JaZtvT08F9buC+35sc9tDYOM1pPJzfu3H3y+Ey3TfrKgMF1nTufPKLvAZx8e/cnBgAA'");
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
