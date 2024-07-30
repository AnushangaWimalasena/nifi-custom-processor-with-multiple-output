/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ansjava.ans.processors.custom;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.BeforeEach;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;


public class MyProcessorTest {

    private TestRunner testRunner;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(MyProcessor.class);
    }

    @org.junit.Test
    public void testProcessorGoodPerson1() {
        // Set the properties
        testRunner.setProperty(MyProcessor.ATTRIBUTE_TO_EXPLODE, "person_1");
        testRunner.setProperty(MyProcessor.EXPLODED_ATTRIBUTE_PREFIX, "");

        // Create a JSON string for the FlowFile content
        String json = "{ \"person_1\": { \"person_type\": \"GOOD\", \"name\": \"John Doe\" }, \"person_2\": { \"person_type\": \"BAD\", \"name\": \"Jane Doe\" } }";

        // Add the content to a FlowFile
        testRunner.enqueue(json);

        // Run the processor
        testRunner.run();

        // Validate the results
        System.out.println("Test rel 1 - Start");
        testRunner.assertTransferCount(MyProcessor.REL_SUCCESS_TYPE_1, 1);
        System.out.println("Test rel 1 - End");
        System.out.println("");
        System.out.println("Test rel 2 - Start");
        testRunner.assertTransferCount(MyProcessor.REL_SUCCESS_TYPE_2, 1);
        System.out.println("Test rel 2 - End");
        System.out.println("");

        // Validate the attributes and content of the transferred FlowFiles
        FlowFile goodPersonFlowFile = testRunner.getFlowFilesForRelationship(MyProcessor.REL_SUCCESS_TYPE_1).get(0);
        assertNotNull(goodPersonFlowFile);
        assertEquals("good", goodPersonFlowFile.getAttribute("person_type"));

        FlowFile badPersonFlowFile = testRunner.getFlowFilesForRelationship(MyProcessor.REL_SUCCESS_TYPE_2).get(0);
        assertNotNull(badPersonFlowFile);
        assertEquals("bad", badPersonFlowFile.getAttribute("person_type"));
    }

    @Test
    public void testProcessorFailure() {
        // Set the properties
        testRunner.setProperty(MyProcessor.ATTRIBUTE_TO_EXPLODE, "non_existing_attribute");
        testRunner.setProperty(MyProcessor.EXPLODED_ATTRIBUTE_PREFIX, "");

        // Create a JSON string for the FlowFile content
        String json = "{ \"person_1\": { \"person_type\": \"UNKNOWN\", \"name\": \"John Doe\" }, \"person_2\": { \"person_type\": \"UNKNOWN\", \"name\": \"Jane Doe\" } }";

        // Add the content to a FlowFile
        testRunner.enqueue(json);

        // Run the processor
        testRunner.run();

        // Validate the results
        testRunner.assertTransferCount(MyProcessor.REL_FAILURE, 1);
    }

}
