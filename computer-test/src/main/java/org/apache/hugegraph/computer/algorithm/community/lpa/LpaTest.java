/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hugegraph.computer.algorithm.community.lpa;

import com.google.common.collect.ImmutableMap;
import org.apache.hugegraph.computer.algorithm.AlgorithmTestBase;
import org.apache.hugegraph.driver.GraphManager;
import org.apache.hugegraph.driver.HugeClient;
import org.apache.hugegraph.driver.SchemaManager;
import org.apache.hugegraph.structure.constant.T;
import org.apache.hugegraph.structure.graph.Vertex;
import org.apache.hugegraph.structure.gremlin.ResultSet;
import org.apache.hugegraph.testutil.Assert;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.Map;

public class LpaTest extends AlgorithmTestBase {

    private static final String VERTX_LABEL = "tc_user";
    private static final String EDGE_LABEL = "tc_know";
    private static final String PROPERTY_KEY = "tc_weight";


    @BeforeClass
    public static void setup() {
        clearAll();

        SchemaManager schema = client().schema();
        schema.propertyKey(PROPERTY_KEY)
                .asInt()
                .ifNotExist()
                .create();
        schema.vertexLabel(VERTX_LABEL)
                .properties(PROPERTY_KEY)
                .useCustomizeStringId()
                .ifNotExist()
                .create();
        schema.edgeLabel(EDGE_LABEL)
                .sourceLabel(VERTX_LABEL)
                .targetLabel(VERTX_LABEL)
                .properties(PROPERTY_KEY)
                .ifNotExist()
                .create();

        GraphManager graph = client().graph();
        Vertex vA = graph.addVertex(T.LABEL, VERTX_LABEL, T.ID, "tc_A",
                PROPERTY_KEY, 1);
        Vertex vB = graph.addVertex(T.LABEL, VERTX_LABEL, T.ID, "tc_B",
                PROPERTY_KEY, 1);
        Vertex vC = graph.addVertex(T.LABEL, VERTX_LABEL, T.ID, "tc_C",
                PROPERTY_KEY, 1);
        Vertex vD = graph.addVertex(T.LABEL, VERTX_LABEL, T.ID, "tc_D",
                PROPERTY_KEY, 1);
        Vertex vE = graph.addVertex(T.LABEL, VERTX_LABEL, T.ID, "tc_E",
                PROPERTY_KEY, 1);

        vA.addEdge(EDGE_LABEL, vB, PROPERTY_KEY, 1);
        vA.addEdge(EDGE_LABEL, vC, PROPERTY_KEY, 1);
        vB.addEdge(EDGE_LABEL, vC, PROPERTY_KEY, 1);
        vC.addEdge(EDGE_LABEL, vD, PROPERTY_KEY, 1);
        vD.addEdge(EDGE_LABEL, vA, PROPERTY_KEY, 1);
        vD.addEdge(EDGE_LABEL, vE, PROPERTY_KEY, 1);
        vE.addEdge(EDGE_LABEL, vD, PROPERTY_KEY, 1);
        vE.addEdge(EDGE_LABEL, vC, PROPERTY_KEY, 1);
    }

    @AfterClass
    public static void teardown() {
        //clearAll();
    }

    @Test
    public void testRunAlgorithm() throws InterruptedException {
        runAlgorithm(LpaParams.class.getName());

        // check result
        HugeClient client =  client();
        ResultSet result  = client.gremlin().gremlin("g.V().group().by('lpa').by(T.id)").execute();

        LinkedHashMap resultRow = (LinkedHashMap)result.data().get(0);
        Assert.assertEquals(4, resultRow.size());

    }
}
