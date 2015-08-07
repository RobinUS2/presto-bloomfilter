package com.facebook.presto.bloomfilter;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.FunctionFactory;
import com.facebook.presto.operator.scalar.FunctionAssertions;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.facebook.presto.type.ParametricType;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.testng.annotations.Test;
import com.facebook.presto.spi.type.BooleanType;

import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static org.testng.Assert.assertNotNull;

public class TestBloomFilterQueries
        extends AbstractTestQueryFramework
{
    public TestBloomFilterQueries()
    {
        super(createLocalQueryRunner());
    }

    @Test
    public void testPrediction()
            throws Exception
    {
        assertQueryTrue("WITH a AS (SELECT bloom_filter('test') AS bf) SELECT bloom_filter_contains(a.bf, 'test') FROM a LIMIT 1");
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
