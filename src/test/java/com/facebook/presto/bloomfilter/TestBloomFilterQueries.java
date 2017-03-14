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
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.AbstractTestQueryFramework.QueryRunnerSupplier;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slices;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.testng.annotations.Test;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;

import static com.facebook.presto.metadata.FunctionExtractor.extractFunctions;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;

public class TestBloomFilterQueries
        extends AbstractTestQueryFramework
{
    public TestBloomFilterQueries()
    {
        super(createQueryRunner());
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
        assertQuery("WITH a AS (SELECT bloom_filter('test') AS bf) SELECT bloom_filter_contains(a.bf, 'test') FROM a LIMIT 1", "SELECT true");

        // Test negative in filter
        assertQuery("WITH a AS (SELECT bloom_filter('test') AS bf) SELECT bloom_filter_contains(a.bf, 'not-in-here') FROM a LIMIT 1", "SELECT false");

        // Test with config (expected insertions)
        assertQuery("WITH a AS (SELECT bloom_filter('test', 10) AS bf) SELECT bloom_filter_contains(a.bf, 'test') FROM a LIMIT 1", "SELECT true");

        // Test with config (expected insertions)
        assertQuery("SELECT get_expected_insertions(bloom_filter('test', 10))", "SELECT 10");

        // Test with config (expected insertions)
        assertQuery("SELECT get_false_positive_percentage(bloom_filter('test', 10, 0.1234))", "SELECT 0.1234");

        // Test with config (expected insertions AND false positive percentage)
        assertQuery("WITH a AS (SELECT bloom_filter('test', 10, 0.001) AS bf) SELECT bloom_filter_contains(a.bf, 'test') FROM a LIMIT 1", "SELECT true");

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

        // Test construction
        assertQuery("WITH a AS (SELECT 'robin' AS uuid), b AS (SELECT bloom_filter(a.uuid) AS bf FROM a), c AS (SELECT to_string(b.bf) AS j FROM b), d AS (SELECT bloom_filter_from_string(c.j) AS bf2 FROM c) SELECT bloom_filter_contains(d.bf2, 'robin'), bloom_filter_contains(d.bf2, 'john') FROM d", "SELECT true, false");
    }

    @Test
    public void testBloomFilterLoadPersist()
            throws Exception
    {
        // Start local server
        int port = 8081;
        Server server = new Server(port);
        Handler handler = new AbstractHandler()
        {
            @Override
            public void handle(String s, Request request, HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse) throws IOException, ServletException
            {
                httpServletResponse.setContentType("text/html");
                if (httpServletRequest.getMethod().equalsIgnoreCase("GET") && httpServletRequest.getRequestURI().contains("bloomfilter/key1")) {
                    // Get
                    byte[] bytes = BloomFilter.newInstance().put(Slices.wrappedBuffer("robin".getBytes())).toBase64();
                    httpServletResponse.setStatus(HttpServletResponse.SC_OK);
                    httpServletResponse.getOutputStream().write(bytes);
                }
                else if (httpServletRequest.getMethod().equalsIgnoreCase("PUT") && httpServletRequest.getRequestURI().contains("bloomfilter/key1")) {
                    // Put
                    httpServletResponse.setStatus(HttpServletResponse.SC_OK);
                }
            }
        };
        server.setHandler(handler);
        server.start();

        // Url
        String url = "http://127.0.0.1:" + port + "/bloomfilter/key1";

        // Test persist
        assertQuery("WITH a AS (SELECT 'robin' AS uuid), b AS (SELECT bloom_filter(a.uuid) AS bf FROM a) SELECT bloom_filter_persist(b.bf, '" + url + "') FROM b", "SELECT true");

        // Test load
        assertQuery("WITH a AS (SELECT bloom_filter_load('" + url + "') AS bf) SELECT bloom_filter_contains(a.bf, 'robin'), bloom_filter_contains(a.bf, 'john') FROM a", "SELECT true, false");

        // Tear down local server
        server.stop();
    }

    private static LocalQueryRunnerSupplier createQueryRunner()
    {
        try {
            Session defaultSession = testSessionBuilder()
                    .setCatalog("local")
                    .setSchema(TINY_SCHEMA_NAME)
                    .build();

            LocalQueryRunner localQueryRunner = new LocalQueryRunner(defaultSession);

            // add the tpch catalog
            // local queries run directly against the generator
            localQueryRunner.createCatalog(
                    defaultSession.getCatalog().get(),
                    new TpchConnectorFactory(1),
                    ImmutableMap.<String, String>of());

            localQueryRunner.getTypeManager().addType(new BloomFilterType());
            localQueryRunner.getTypeManager().addParametricType(new BloomFilterParametricType());
            localQueryRunner.getMetadata().addFunctions(extractFunctions(new BloomFilterPlugin().getFunctions()));

            return new LocalQueryRunnerSupplier(localQueryRunner);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static class LocalQueryRunnerSupplier implements QueryRunnerSupplier
    {
        private final LocalQueryRunner queryRunner;

        public LocalQueryRunnerSupplier(LocalQueryRunner queryRunner)
        {
            this.queryRunner = queryRunner;
        }

        public QueryRunner get()
        {
            return queryRunner;
        }
    }
}
