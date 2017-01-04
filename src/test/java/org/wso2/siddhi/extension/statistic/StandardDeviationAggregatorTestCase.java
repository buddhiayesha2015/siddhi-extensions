/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


package org.wso2.siddhi.extension.statistic;

import org.apache.log4j.Logger;
import org.junit.Test;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;

public class StandardDeviationAggregatorTestCase {

    private static final Logger log = Logger.getLogger(StandardDeviationAggregatorTestCase.class);

    @Test
    public void timeWindowBatchTest1() throws InterruptedException {

        System.out.println("\nSD TestCase 1 (Double)");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inputStream = "" +
                "define stream inputStream (time long, val double);";
        String query = "" +
                "@info(name = 'query1') " +
                "from inputStream " +
                "select time, statistic:sd(val) as sd " +
                "insert into outputStream;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inputStream + query);

        executionPlanRuntime.addCallback("outputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                for (Event ev : events) {
                    System.out.println("time: " + ev.getData()[0] + "\tsd: " + ev.getData()[1]);
                }
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();

        inputHandler.send(new Object[]{100l, 3d});
        inputHandler.send(new Object[]{101l, 3d});
        inputHandler.send(new Object[]{102l, 5d});

        executionPlanRuntime.shutdown();
    }

    @Test
    public void timeWindowBatchTest2() throws InterruptedException {

        System.out.println("\nSD TestCase 2 (Long)");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inputStream = "" +
                "define stream inputStream (time long, val long);";
        String query = "" +
                "@info(name = 'query1') " +
                "from inputStream " +
                "select time, statistic:sd(val) as sd " +
                "insert into outputStream;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inputStream + query);

        executionPlanRuntime.addCallback("outputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                for (Event ev : events) {
                    System.out.println("time: " + ev.getData()[0] + "\tsd: " + ev.getData()[1]);
                }
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();

        inputHandler.send(new Object[]{100l, 3l});
        inputHandler.send(new Object[]{101l, 3l});
        inputHandler.send(new Object[]{102l, 5l});

        executionPlanRuntime.shutdown();
    }

    @Test
    public void timeWindowBatchTest3() throws InterruptedException {

        System.out.println("\nSD TestCase 3 (Integer)");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inputStream = "" +
                "define stream inputStream (time long, val int);";
        String query = "" +
                "@info(name = 'query1') " +
                "from inputStream " +
                "select time, statistic:sd(val) as sd " +
                "insert into outputStream;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inputStream + query);

        executionPlanRuntime.addCallback("outputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                for (Event ev : events) {
                    System.out.println("time: " + ev.getData()[0] + "\tsd: " + ev.getData()[1]);
                }
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();

        inputHandler.send(new Object[]{100l, 9});
        inputHandler.send(new Object[]{101l, 10});
        inputHandler.send(new Object[]{102l, 11});
        inputHandler.send(new Object[]{100l, 7});
        inputHandler.send(new Object[]{101l, 13});

        executionPlanRuntime.shutdown();
    }

    @Test
    public void timeWindowBatchTest4() throws InterruptedException {

        System.out.println("\nSD TestCase 4 (Float)");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inputStream = "" +
                "define stream inputStream (time long, val float);";
        String query = "" +
                "@info(name = 'query1') " +
                "from inputStream " +
                "select time, statistic:sd(val) as sd " +
                "insert into outputStream;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inputStream + query);

        executionPlanRuntime.addCallback("outputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                for (Event ev : events) {
                    System.out.println("time: " + ev.getData()[0] + "\tsd: " + ev.getData()[1]);
                }
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();

        inputHandler.send(new Object[]{100l, 3f});
        inputHandler.send(new Object[]{101l, 3f});
        inputHandler.send(new Object[]{102l, 5f});

        executionPlanRuntime.shutdown();
    }

}
