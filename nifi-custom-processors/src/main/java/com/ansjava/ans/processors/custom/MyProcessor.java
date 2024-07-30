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

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.*;
import java.util.*;

//import java.util.Iterator;
//import java.util.Map;
//
//import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.*;

//import org.apache.nifi.components.PropertyDescriptor;
//import org.apache.nifi.flowfile.FlowFile;
//import org.apache.nifi.processor.AbstractProcessor;
//import org.apache.nifi.processor.ProcessContext;
//import org.apache.nifi.processor.ProcessSession;
//import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.io.StreamCallback;
//import org.apache.nifi.processor.exception.ProcessException;

import java.io.IOException;
//import java.util.Collections;
import java.util.List;
import java.util.Set;

@Tags({"example"})
@CapabilityDescription("Provide a description")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class MyProcessor extends AbstractProcessor {
    public static final Relationship REL_FAILURE = new Relationship
            .Builder()
            .description("Failed adding attributes")
            .name("failure")
            .build();

    public static final Relationship REL_SUCCESS_TYPE_1 = new Relationship
            .Builder()
            .description("Added attributes to flow file 1")
            .name("type_1")
            .build();


    public static final Relationship REL_SUCCESS_TYPE_2 = new Relationship
            .Builder()
            .description("Added attributes to flow file 2")
            .name("type_2")
            .build();


    public static final Relationship REL_ORG = new Relationship
            .Builder()
            .description("Original file")
            .name("org")
            .build();


    public static final PropertyDescriptor ATTRIBUTE_TO_EXPLODE = new PropertyDescriptor
            .Builder()
            .name("attributeToExplode")
            .displayName("Attribute To Explode")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR)
            .build();

    public static final PropertyDescriptor EXPLODED_ATTRIBUTE_PREFIX = new PropertyDescriptor
            .Builder()
            .name("explodedAttributePrefix")
            .displayName("Exploded Attribute Prefix")
            .defaultValue("")
            .addValidator(StandardValidators.ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR)
            .build();

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return new ArrayList<PropertyDescriptor>() {{
            add(ATTRIBUTE_TO_EXPLODE);
            add(EXPLODED_ATTRIBUTE_PREFIX);
        }};
    }

    @Override
    public Set<Relationship> getRelationships() {
        return new HashSet<Relationship>() {{
            add(REL_FAILURE);
            add(REL_ORG);
            add(REL_SUCCESS_TYPE_1);
            add(REL_SUCCESS_TYPE_2);
        }};
    }

    private String getContent(FlowFile flowFile, ProcessSession session) {
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        session.exportTo(flowFile, byteArrayOutputStream);
        return byteArrayOutputStream.toString();
    }


    private void handlePerson(ProcessSession session, JSONObject personObject, String personType, String prefix) throws IOException {
        if ("GOOD".equals(personType)) {
            System.out.println("========================== GOOD EX ==========================");
            createAndTransferFlowFile(session, personObject, REL_SUCCESS_TYPE_1, prefix, "good");
            System.out.println("====================================");
            System.out.println("");
        } else if ("BAD".equals(personType)) {
            System.out.println("========================== BAD EX ==========================");
            createAndTransferFlowFile(session, personObject, REL_SUCCESS_TYPE_2, prefix, "bad");
            System.out.println("====================================");
            System.out.println("");
        }
    }

    private void createAndTransferFlowFile(ProcessSession session, JSONObject personObject, Relationship relationship, String prefix, String type) throws IOException {
        FlowFile newFlowFile = session.create();
        String content = personObject.toString();
        System.out.println("String content:"+ content);
        System.out.println("relationship:"+ relationship.toString());
        newFlowFile = session.write(newFlowFile, out -> out.write(content.getBytes()));
        session.putAttribute(newFlowFile, prefix + "person_type", type);
        session.transfer(newFlowFile, relationship);
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) {
        // read flowFile
        FlowFile flowFile = session.get();

        // read properties
//        String attributeToExplode = context.getProperty(ATTRIBUTE_TO_EXPLODE).getValue();
        String explodedAttributePrefix = context.getProperty(EXPLODED_ATTRIBUTE_PREFIX).getValue();


        try {
            String jsonString =  getContent(flowFile, session);
            System.out.println("Json XString:"+jsonString);
            // Parse the JSON string
            JSONParser parser = new JSONParser();
            Object obj = parser.parse(jsonString);

            // Convert Object to JSONObject
            JSONObject jsonObject = (JSONObject) obj;


            JSONObject personObj1JsonObject = new JSONObject();
            personObj1JsonObject = (JSONObject) jsonObject.get("person_1");
            System.out.println("Json person 1:"+personObj1JsonObject.toString());

            JSONObject personObj2JsonObject = new JSONObject();
            personObj2JsonObject = (JSONObject) jsonObject.get("person_2");
            System.out.println("Json person 2:"+personObj2JsonObject.toString());

            String person1Type = (String) personObj1JsonObject.get("person_type");
            String person2Type = (String) personObj2JsonObject.get("person_type");

            String[] personTypeArry = {"GOOD", "BAD"};
            List personTypeList = Arrays.asList(personTypeArry);

            if ( !( (personTypeList.contains(person1Type)) & (personTypeList.contains(person2Type)) )  ){
                System.out.println("Error happens");
                throw new Exception("Person type is not defined");
            }
            handlePerson(session, personObj1JsonObject, person1Type, explodedAttributePrefix);
            handlePerson(session, personObj2JsonObject, person2Type, explodedAttributePrefix);

            session.transfer(flowFile, REL_ORG);
        } catch (Exception e) {
            session.transfer(flowFile, REL_FAILURE);
        }
    }
}