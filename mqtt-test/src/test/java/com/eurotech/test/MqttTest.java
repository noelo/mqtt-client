/*
 * Copyright (c) 2013 Eurotech.
 */
package com.eurotech.test;


import junit.framework.TestCase;
import org.apache.log4j.Logger;
import org.fusesource.hawtbuf.Buffer;

import org.fusesource.mqtt.client.*;
import org.fusesource.mqtt.codec.MQTTFrame;
import org.fusesource.mqtt.codec.PUBLISH;

import org.junit.After;
import org.junit.Before;

import java.util.concurrent.TimeUnit;

import static org.fusesource.mqtt.client.QoS.*;

public class MqttTest extends TestCase implements MqttTestConstants {
    private static Logger LOG = Logger.getLogger(MqttTest.class);


    private MQTT client;
    private BlockingConnection connection;
    private String expectedResult;
    private Message result;

    @Before
    public void setUp() throws Exception {
        client = new MQTT();
        client.setCleanSession(true);
//        client.setTracer(new Tracer() {
//            @Override
//            public void onReceive(MQTTFrame frame) {
//                if (frame == null) return;
//                Buffer myBuffer = frame.buffers[0].deepCopy();
//                MQTTFrame myFrame = new MQTTFrame(myBuffer);
//
//                PUBLISH publish = null;
//                try {
//                    publish = new PUBLISH().decode(myFrame);
//                } catch (Exception ex) {
//                    //
//                }
//                LOG.info("Primary Client Received: " + frame + " payload:" + publish.toString());
//            }
//
//            @Override
//            public void onSend(MQTTFrame frame) {
//                LOG.info("Primary Client Sent:" + frame);
//            }
//
//            @Override
//            public void debug(String message, Object... args) {
//                LOG.info(String.format(message, args));
//            }
//        });

        client.setHost(BROKER_URL);
        client.setClientId(CLIENT_ID);
        client.setConnectAttemptsMax(1);
        client.setReconnectAttemptsMax(0);
        connection = client.blockingConnection();
        connection.connect();
    }

    @After
    public void tearDown() throws Exception {
        if (connection != null && connection.isConnected()) {
            connection.disconnect();
            while (connection.isConnected()) {
                Thread.sleep(TIMEOUT);
            }
        }
    }

    public void testBasics() throws Exception {

        // check basic pub/sub on QOS 0
        LOG.info("check basic pub/sub on QOS 0");
        expectedResult = "hello mqtt broker on QOS 0";
        result = null;
        connection.subscribe(new Topic[]{new Topic("1/2/3", AT_MOST_ONCE)});
        connection.publish("1/2/3", expectedResult.getBytes(), AT_MOST_ONCE, false);
        Thread.sleep(TIMEOUT);
        result = connection.receive(TIMEOUT, TimeUnit.MILLISECONDS);
        assertEquals(expectedResult, new String(result.getPayload(), "UTF-8"));
        result.ack();
        connection.unsubscribe(new String[]{"1/2/3"});

        // check basic pub/sub on QOS 1
        LOG.info("check basic pub/sub on QOS 1");
        expectedResult = "hello mqtt broker on QOS 1";
        result = null;
        connection.subscribe(new Topic[]{new Topic("a/b/c", AT_LEAST_ONCE)});
        connection.publish("a/b/c", expectedResult.getBytes(), AT_LEAST_ONCE, false);
        Thread.sleep(TIMEOUT);
        result = connection.receive(TIMEOUT, TimeUnit.MILLISECONDS);
        assertEquals(expectedResult, new String(result.getPayload(), "UTF-8"));
        result.ack();
        connection.unsubscribe(new String[]{"a/b/c"});

        // check basic pub/sub on QOS 2
        LOG.info("check basic pub/sub on QOS 2");
        expectedResult = "hello mqtt broker on QOS 2";
        result = null;
        connection.subscribe(new Topic[]{new Topic("1/2", EXACTLY_ONCE)});
        connection.publish("1/2", expectedResult.getBytes(), EXACTLY_ONCE, false);
        Thread.sleep(TIMEOUT);
        result = connection.receive(TIMEOUT, TimeUnit.MILLISECONDS);
        assertEquals(expectedResult, new String(result.getPayload(), "UTF-8"));
        result.ack();
        connection.unsubscribe(new String[]{"1/2"});
    }

    public void testLongTopic() throws Exception {
        // *****************************************
        // check a simple # subscribe works
        // *****************************************
        result = null;
        connection.subscribe(new Topic[]{new Topic("1/2/3/this_is_a_long_topic_that_wasnt_working/before/4/5/6/7/8", AT_MOST_ONCE)});
        expectedResult = "hello mqtt broker on long topic";
        result = null;
        connection.publish(
                "1/2/3/this_is_a_long_topic_that_wasnt_working/before/4/5/6/7/8",
                expectedResult.getBytes(), AT_MOST_ONCE, false);
        Thread.sleep(TIMEOUT);
        result = connection.receive(TIMEOUT, TimeUnit.MILLISECONDS);
        assertEquals(expectedResult, new String(result.getPayload(), "UTF-8"));
        result.ack();

        connection.unsubscribe(new String[]{"1/2/3/this_is_a_long_topic_that_wasnt_working/before/4/5/6/7/8"});
        connection.subscribe(new Topic[]{new Topic("1/2/3/this_is_a_long_topic_that_wasnt_working/before/4/5/6/7/8/#", AT_MOST_ONCE)});

        expectedResult = "hello mqtt broker on long topic with hash";
        result = null;
        connection.publish(
                "1/2/3/this_is_a_long_topic_that_wasnt_working/before/4/5/6/7/8",
                expectedResult.getBytes(), AT_MOST_ONCE, false);
        Thread.sleep(TIMEOUT);
        result = connection.receive(TIMEOUT, TimeUnit.MILLISECONDS);
        assertEquals(expectedResult, new String(result.getPayload(), "UTF-8"));
        result.ack();

        expectedResult = "hello mqtt broker on long topic with hash again";
        result = null;
        connection.publish(
                "1/2/3/this_is_a_long_topic_that_wasnt_working/before/4/5/6/7/8/9/10/0",
                expectedResult.getBytes(), AT_MOST_ONCE, false);
        Thread.sleep(TIMEOUT);
        result = connection.receive(TIMEOUT, TimeUnit.MILLISECONDS);
        assertEquals(expectedResult, new String(result.getPayload(), "UTF-8"));
        result.ack();

        connection.unsubscribe(new String[]{"1/2/3/this_is_a_long_topic_that_wasnt_working/before/4/5/6/7/8/#"});
    }

    public void testOverlappingTopics() throws Exception {

        // *****************************************
        // check a simple # subscribe works
        // *****************************************

        result = null;

        connection.subscribe(new Topic[]{new Topic("#", AT_MOST_ONCE)});
        expectedResult = "hello mqtt broker on hash";
        result = null;
        connection.publish("a/b/c", expectedResult.getBytes(), AT_MOST_ONCE, false);
        Thread.sleep(TIMEOUT);
        result = connection.receive(TIMEOUT, TimeUnit.MILLISECONDS);
        assertEquals(expectedResult, new String(result.getPayload(), "UTF-8"));
        result.ack();

        expectedResult = "hello mqtt broker on a different topic";
        result = null;
        connection.publish("1/2/3/4/5/6", expectedResult.getBytes(), AT_MOST_ONCE, false);
        Thread.sleep(TIMEOUT);
        result = connection.receive(TIMEOUT, TimeUnit.MILLISECONDS);
        assertEquals(expectedResult, new String(result.getPayload(), "UTF-8"));
        result.ack();

        // *****************************************
        // now subscribe on a topic that overlaps the root # wildcard - we
        // should still get everything
        // *****************************************
        connection.subscribe(new Topic[]{new Topic("1/2/3", AT_MOST_ONCE)});
        expectedResult = "hello mqtt broker on explicit topic";
        result = null;
        connection.publish("1/2/3", expectedResult.getBytes(), AT_MOST_ONCE, false);
        Thread.sleep(TIMEOUT);
        result = connection.receive(TIMEOUT, TimeUnit.MILLISECONDS);
        assertEquals(expectedResult, new String(result.getPayload(), "UTF-8"));
        result.ack();

        expectedResult = "hello mqtt broker on some other topic";
        result = null;
        connection.publish("a/b/c/d/e", expectedResult.getBytes(), AT_MOST_ONCE, false);
        Thread.sleep(TIMEOUT);
        result = connection.receive(TIMEOUT, TimeUnit.MILLISECONDS);
        assertEquals(expectedResult, new String(result.getPayload(), "UTF-8"));
        result.ack();

        // *****************************************
        // now unsub hash - we should only get called back on 1/2/3
        // *****************************************

        connection.unsubscribe(new String[]{"#"});
        expectedResult = "this should not come back...";
        result = null;
        connection.publish("1/2/3/4", expectedResult.getBytes(), AT_MOST_ONCE, false);
        Thread.sleep(TIMEOUT);
        result = connection.receive(TIMEOUT, TimeUnit.MILLISECONDS);
        assertNull(result);


        expectedResult = "this should not come back either...";
        result = null;
        connection.publish("a/b/c", expectedResult.getBytes(), AT_MOST_ONCE, false);
        Thread.sleep(TIMEOUT);
        result = connection.receive(TIMEOUT, TimeUnit.MILLISECONDS);
        assertNull(result);


        // *****************************************
        // this should still come back since we are still subscribed on 1/2/3
        // *****************************************
        expectedResult = "we should still get this";
        result = null;
        connection.publish("1/2/3", expectedResult.getBytes(), AT_MOST_ONCE, false);
        Thread.sleep(TIMEOUT);
        result = connection.receive(TIMEOUT, TimeUnit.MILLISECONDS);
        String receivedResult = new String(result.getPayload(), "UTF-8");
        assertEquals("No message received on 1/2/3 after unsubscribe of '#'", expectedResult, receivedResult);
        result.ack();


        // *****************************************
        // remove the remaining subscription
        // *****************************************
        connection.unsubscribe(new String[]{"1/2/3"});

        // *****************************************
        // repeat the above full test but reverse the order of the subs
        // *****************************************
        connection.subscribe(new Topic[]{new Topic("1/2/3", AT_MOST_ONCE)});
        expectedResult = "hello mqtt broker on hash";
        result = null;
        connection.publish("1/2/3", expectedResult.getBytes(), AT_MOST_ONCE, false);
        Thread.sleep(TIMEOUT);
        result = connection.receive(TIMEOUT, TimeUnit.MILLISECONDS);
        assertEquals(expectedResult, new String(result.getPayload(), "UTF-8"));
        result.ack();


        expectedResult = "hello mqtt broker on a different topic - we shouldn't get this";
        result = null;
        connection.publish("1/2/3/4/5/6", expectedResult.getBytes(), AT_MOST_ONCE, false);
        Thread.sleep(TIMEOUT);
        assertNull(result);
        result = connection.receive(TIMEOUT, TimeUnit.MILLISECONDS);
        assertNull(result);

        connection.subscribe(new Topic[]{new Topic("#", AT_MOST_ONCE)});
        expectedResult = "hello mqtt broker on some other topic topic";
        result = null;
        connection.publish("a/b/c/d", expectedResult.getBytes(), AT_MOST_ONCE, false);
        Thread.sleep(TIMEOUT);
        result = connection.receive(TIMEOUT, TimeUnit.MILLISECONDS);
        assertEquals(expectedResult, new String(result.getPayload(), "UTF-8"));
        result.ack();

        expectedResult = "hello mqtt broker on some other topic";
        result = null;
        connection.publish("1/2/3/4/5/6", expectedResult.getBytes(), AT_MOST_ONCE, false);
        Thread.sleep(TIMEOUT);
        result = connection.receive(TIMEOUT, TimeUnit.MILLISECONDS);
        assertEquals(expectedResult, new String(result.getPayload(), "UTF-8"));
        result.ack();

        connection.unsubscribe(new String[]{"1/2/3"});

        expectedResult = "this should come back...";
        result = null;
        connection.publish("1/2/3/4", expectedResult.getBytes(), AT_MOST_ONCE, false);
        Thread.sleep(TIMEOUT);
        result = connection.receive(TIMEOUT, TimeUnit.MILLISECONDS);
        assertEquals(expectedResult, new String(result.getPayload(), "UTF-8"));
        result.ack();

        expectedResult = "this should come back too...";
        result = null;
        connection.publish("a/b/c", expectedResult.getBytes(), AT_MOST_ONCE, false);
        Thread.sleep(TIMEOUT);
        result = connection.receive(TIMEOUT, TimeUnit.MILLISECONDS);
        assertEquals(expectedResult, new String(result.getPayload(), "UTF-8"));
        result.ack();

        expectedResult = "we should still get this as well";
        result = null;
        connection.publish("1/2/3", expectedResult.getBytes(), AT_MOST_ONCE, false);
        Thread.sleep(TIMEOUT);
        result = connection.receive(TIMEOUT, TimeUnit.MILLISECONDS);
        assertEquals(expectedResult, new String(result.getPayload(), "UTF-8"));
        result.ack();

        connection.unsubscribe(new String[]{"#"});
    }

    public void testDots() throws Exception {

        // *****************************************
        // check that dots are not treated differently
        // *****************************************
        connection.subscribe(new Topic[]{new Topic("1/2/./3", AT_MOST_ONCE)});
        expectedResult = "hello mqtt broker with a dot";
        result = null;
        connection.publish("1/2/./3", expectedResult.getBytes(), AT_MOST_ONCE, false);
        Thread.sleep(TIMEOUT);
        result = connection.receive(TIMEOUT, TimeUnit.MILLISECONDS);
        assertEquals(expectedResult, new String(result.getPayload(), "UTF-8"));
        result.ack();


        expectedResult = "should not get this";
        result = null;
        connection.publish("1/2", expectedResult.getBytes(), AT_MOST_ONCE, false);
        Thread.sleep(TIMEOUT);
        result = connection.receive(TIMEOUT, TimeUnit.MILLISECONDS);
        assertNull(result);


        expectedResult = "should not get this";
        result = null;
        connection.publish("1/2/3", expectedResult.getBytes(), AT_MOST_ONCE, false);
        Thread.sleep(TIMEOUT);
        result = connection.receive(TIMEOUT, TimeUnit.MILLISECONDS);
        assertNull(result);


        expectedResult = "should not get this";
        result = null;
        connection.publish("1/2/./3/4", expectedResult.getBytes(), AT_MOST_ONCE, false);
        Thread.sleep(TIMEOUT);
        result = connection.receive(TIMEOUT, TimeUnit.MILLISECONDS);
        assertNull(result);

        connection.unsubscribe(new String[]{"1/2/./3"});

        // *****************************************
        // check that we really are unsubscribed now
        // *****************************************
        expectedResult = "should not get this";
        result = null;
        connection.publish("1/2/./3", expectedResult.getBytes(), AT_MOST_ONCE, false);
        Thread.sleep(TIMEOUT);
        result = connection.receive(TIMEOUT, TimeUnit.MILLISECONDS);
        assertNull(result);

        expectedResult = "should not get this";
        result = null;
        connection.publish("1/2/3", expectedResult.getBytes(), AT_MOST_ONCE, false);
        Thread.sleep(TIMEOUT);
        result = connection.receive(TIMEOUT, TimeUnit.MILLISECONDS);
        assertNull(result);

        expectedResult = "should not get this";
        result = null;
        connection.publish("1/2/./3/4", expectedResult.getBytes(), AT_MOST_ONCE, false);
        Thread.sleep(TIMEOUT);
        result = connection.receive(TIMEOUT, TimeUnit.MILLISECONDS);
        assertNull(result);
    }

    public void testActiveMQWildcards() throws Exception {

        // *****************************************
        // check that ActiveMQ native wildcards are not treated differently
        // *****************************************

        connection.subscribe(new Topic[]{new Topic("*/>/#", AT_MOST_ONCE)});
        expectedResult = "hello mqtt broker with fake wildcards";
        result = null;
        connection.publish("*/>/1/2/3", expectedResult.getBytes(), AT_MOST_ONCE, false);
        Thread.sleep(TIMEOUT);
        result = connection.receive(TIMEOUT, TimeUnit.MILLISECONDS);
        assertEquals(expectedResult, new String(result.getPayload(), "UTF-8"));
        result.ack();


        expectedResult = "should not get this";
        result = null;
        connection.publish("1", expectedResult.getBytes(), AT_MOST_ONCE, false);
        Thread.sleep(TIMEOUT);
        result = connection.receive(TIMEOUT, TimeUnit.MILLISECONDS);
        assertNull(result);


        expectedResult = "should not get this";
        result = null;
        connection.publish("1/2", expectedResult.getBytes(), AT_MOST_ONCE, false);
        Thread.sleep(TIMEOUT);
        result = connection.receive(TIMEOUT, TimeUnit.MILLISECONDS);
        assertNull(result);


        expectedResult = "should not get this";
        result = null;
        connection.publish("1/2/3", expectedResult.getBytes(), AT_MOST_ONCE, false);
        Thread.sleep(TIMEOUT);
        result = connection.receive(TIMEOUT, TimeUnit.MILLISECONDS);
        assertNull(result);

        expectedResult = "should not get this";
        result = null;
        connection.publish("*/2", expectedResult.getBytes(), AT_MOST_ONCE, false);
        Thread.sleep(TIMEOUT);
        result = connection.receive(TIMEOUT, TimeUnit.MILLISECONDS);
        assertNull(result);


        expectedResult = "should get this";
        result = null;
        connection.publish("*/>/3", expectedResult.getBytes(), AT_MOST_ONCE, false);
        Thread.sleep(TIMEOUT);
        result = connection.receive(TIMEOUT, TimeUnit.MILLISECONDS);
        assertEquals(expectedResult, new String(result.getPayload(), "UTF-8"));
        result.ack();


        expectedResult = "should get this";
        result = null;
        connection.publish("*/>", expectedResult.getBytes(), AT_MOST_ONCE, false);
        Thread.sleep(TIMEOUT);
        result = connection.receive(TIMEOUT, TimeUnit.MILLISECONDS);
        assertEquals(expectedResult, new String(result.getPayload(), "UTF-8"));
        result.ack();

    }

    public void testNativeMQTTWildcards() throws Exception {

        // *****************************************
        // check that hash works right with plus
        // *****************************************
        connection.subscribe(new Topic[]{new Topic("a/+/#", AT_MOST_ONCE)});

        expectedResult = "sub on everything below a but not a";
        result = null;
        connection.publish("a/b", expectedResult.getBytes(), AT_MOST_ONCE, false);
        Thread.sleep(TIMEOUT);
        result = connection.receive(TIMEOUT, TimeUnit.MILLISECONDS);
        assertEquals(expectedResult, new String(result.getPayload(), "UTF-8"));
        result.ack();


        expectedResult = "should not get this";
        result = null;
        connection.publish("a", expectedResult.getBytes(), AT_MOST_ONCE, false);
        Thread.sleep(TIMEOUT);
        result = connection.receive(TIMEOUT, TimeUnit.MILLISECONDS);
        assertNull(result);


        // change sub to just a/#
        connection.unsubscribe(new String[]{"a/+/#"});
        connection.subscribe(new Topic[]{new Topic("a/#", AT_MOST_ONCE)});

        expectedResult = "sub on everything below a including a";
        result = null;
        connection.publish("a/b", expectedResult.getBytes(), AT_MOST_ONCE, false);
        Thread.sleep(TIMEOUT);
        result = connection.receive(TIMEOUT, TimeUnit.MILLISECONDS);
        assertEquals(expectedResult, new String(result.getPayload(), "UTF-8"));
        result.ack();


        expectedResult = "sub on everything below a still including a";
        result = null;
        connection.publish("a", expectedResult.getBytes(), AT_MOST_ONCE, false);
        Thread.sleep(TIMEOUT);
        result = connection.receive(TIMEOUT, TimeUnit.MILLISECONDS);
        assertEquals(expectedResult, new String(result.getPayload(), "UTF-8"));
        result.ack();


        expectedResult = "sub on everything below a still including a - should not get b";
        result = null;
        connection.publish("b", expectedResult.getBytes(), AT_MOST_ONCE, false);
        Thread.sleep(TIMEOUT);
        result = connection.receive(TIMEOUT, TimeUnit.MILLISECONDS);
        assertNull(result);


        connection.unsubscribe(new String[]{"a/#"});
    }

    public void testWildcardPlus() throws Exception {

        // *****************************************
        // check that unsub of hash doesn't affect other subscriptions
        // *****************************************
        connection.subscribe(new Topic[]{new Topic("+/+/+", AT_MOST_ONCE)});
        expectedResult = "should get this 1";
        result = null;
        connection.publish("1/2/3", expectedResult.getBytes(), AT_MOST_ONCE, false);
        Thread.sleep(TIMEOUT);
        result = connection.receive(TIMEOUT, TimeUnit.MILLISECONDS);
        assertEquals(expectedResult, new String(result.getPayload(), "UTF-8"));
        result.ack();


        expectedResult = "should get this 2";
        result = null;
        connection.publish("a/2/3", expectedResult.getBytes(), AT_MOST_ONCE, false);
        Thread.sleep(TIMEOUT);
        result = connection.receive(TIMEOUT, TimeUnit.MILLISECONDS);
        assertEquals(expectedResult, new String(result.getPayload(), "UTF-8"));
        result.ack();


        expectedResult = "should get this 3";
        result = null;
        connection.publish("1/b/c", expectedResult.getBytes(), AT_MOST_ONCE, false);
        Thread.sleep(TIMEOUT);
        result = connection.receive(TIMEOUT, TimeUnit.MILLISECONDS);
        assertEquals(expectedResult, new String(result.getPayload(), "UTF-8"));
        result.ack();


        expectedResult = "should not get this";
        result = null;
        connection.publish("1/2", expectedResult.getBytes(), AT_MOST_ONCE, false);
        Thread.sleep(TIMEOUT);
        result = connection.receive(TIMEOUT, TimeUnit.MILLISECONDS);
        assertNull(result);


        expectedResult = "should not get this either";
        result = null;
        connection.publish("1/2/3/4", expectedResult.getBytes(), AT_MOST_ONCE, false);
        Thread.sleep(TIMEOUT);
        result = connection.receive(TIMEOUT, TimeUnit.MILLISECONDS);
        assertNull(result);

    }

    public void testSubs() throws Exception {
        connection.subscribe(new Topic[]{new Topic("1/2/3", AT_MOST_ONCE)});
        connection.subscribe(new Topic[]{new Topic("a/+/#", AT_MOST_ONCE)});
        connection.subscribe(new Topic[]{new Topic("#", AT_MOST_ONCE)});


        expectedResult = "should get everything";
        result = null;
        connection.publish("1/2/3/4", expectedResult.getBytes(), AT_MOST_ONCE, false);
        Thread.sleep(TIMEOUT);
        result = connection.receive(TIMEOUT, TimeUnit.MILLISECONDS);
        assertEquals(expectedResult, new String(result.getPayload(), "UTF-8"));
        result.ack();


        expectedResult = "should get everything";
        result = null;
        connection.publish("a/1/2", expectedResult.getBytes(), AT_MOST_ONCE, false);
        Thread.sleep(TIMEOUT);
        result = connection.receive(TIMEOUT, TimeUnit.MILLISECONDS);
        assertEquals(expectedResult, new String(result.getPayload(), "UTF-8"));
        result.ack();

        connection.unsubscribe(new String[]{"a/+/#"});
        connection.unsubscribe(new String[]{"#"});

        expectedResult = "should still get 1/2/3";
        result = null;
        connection.publish("1/2/3", expectedResult.getBytes(), AT_MOST_ONCE, false);
        Thread.sleep(TIMEOUT);
        result = connection.receive(TIMEOUT, TimeUnit.MILLISECONDS);
        assertEquals(expectedResult, new String(result.getPayload(), "UTF-8"));
        result.ack();


        expectedResult = "should not get anything else";
        result = null;
        connection.publish("a/2/3", expectedResult.getBytes(), AT_MOST_ONCE, false);
        Thread.sleep(TIMEOUT);
        result = connection.receive(TIMEOUT, TimeUnit.MILLISECONDS);
        assertNull(result);


        expectedResult = "should not get anything else";
        result = null;
        connection.publish("a", expectedResult.getBytes(), AT_MOST_ONCE, false);
        Thread.sleep(TIMEOUT);
        result = connection.receive(TIMEOUT, TimeUnit.MILLISECONDS);
        assertNull(result);

    }

    public void testDupClientId() throws Exception {

        MQTT client2;
        BlockingConnection connection2 = null;
        try {
//            MqttConnectOptions options = new MqttConnectOptions();
//            options.setCleanSession(true);
//
//            StringBuffer sb = new StringBuffer();
//            sb.append(System.getProperty("java.io.tmpdir"));
//            sb.append(System.getProperty("file.separator"));
//            sb.append("client2");
//            String tmpDir2 = sb.toString();
//            MqttDefaultFilePersistence dataStore2 = new MqttDefaultFilePersistence(
//                    tmpDir2);


            client2 = new MQTT();
            client2.setCleanSession(true);
            client2.setHost(BROKER_URL);
            client2.setClientId(CLIENT_ID);
            client2.setConnectAttemptsMax(1);
            connection2 = client2.blockingConnection();
            connection2.connect();


            Thread.sleep(TIMEOUT);

            // at this point, client2 should be connected and client should not

            assertTrue(connection2.isConnected());
            assertFalse(connection.isConnected());
        } catch (Exception e) {
            // we expect an exception to be received
            e.printStackTrace();
            throw e;
        } finally {
            if (connection2 != null && connection2.isConnected()) {
                connection2.disconnect();
                while (connection2.isConnected()) {
                    Thread.sleep(TIMEOUT);
                }
            }
        }
    }

    public void testCleanSession() throws Exception {

        MQTT client2 = null;
        BlockingConnection connection2 = null;
        try {
            client2 = new MQTT();
            client2.setCleanSession(false);
            client2.setHost(BROKER_URL);
            client2.setClientId(CLIENT_ID_2);
            client2.setWillMessage("This is an LWT");
            client2.setWillTopic("l/w/t");
            client2.setWillQos(AT_LEAST_ONCE);
            client2.setWillRetain(false);
            client2.setKeepAlive((short) CONNECTION_TIMEOUT);

            connection2 = client2.blockingConnection();
            connection2.connect();

            // subscribe and wait for the retain message to arrive
            connection2.subscribe(new Topic[]{new Topic("a/b/c", AT_LEAST_ONCE)});
            Thread.sleep(TIMEOUT);
            result = connection2.receive(TIMEOUT, TimeUnit.MILLISECONDS);


            // disconnect
            connection2.disconnect();
            while (connection2.isConnected()) {
                Thread.sleep(TIMEOUT);
            }

            // publish message with retain
            expectedResult = "should not get anything on publish only after the subscribe";
            result = null;
            connection.publish("a/b/c", expectedResult.getBytes(), AT_LEAST_ONCE, false);

            // reconnect with client2 and wait for the pending messages to be
            // sent

            connection2 = client2.blockingConnection();
            connection2.connect();

            Thread.sleep(TIMEOUT * 2);
            result = connection2.receive(TIMEOUT, TimeUnit.MILLISECONDS);
            assertEquals(expectedResult, new String(result.getPayload(), "UTF-8"));
            result.ack();

        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            if (connection2 != null && connection2.isConnected())
                connection2.disconnect();
            while (connection2.isConnected()) {
                Thread.sleep(TIMEOUT);
            }
        }
    }

    public void testRetain() throws Exception {

        // publish with retain
        // publish message with retain
        expectedResult = "should not get anything on publish only after the subscribe";
        result = null;
        connection.publish("a/b/c", expectedResult.getBytes(), AT_LEAST_ONCE, true);
        Thread.sleep(TIMEOUT);
        result = connection.receive(TIMEOUT, TimeUnit.MILLISECONDS);
        assertNull(result);


        MQTT client2 = null;
        BlockingConnection connection2 = null;
        try {

            client2 = new MQTT();
            client2.setCleanSession(true);
            client2.setKeepAlive((short) CONNECTION_TIMEOUT);
            client2.setHost(BROKER_URL);
            client2.setClientId(CLIENT_ID_2);
            connection2 = client2.blockingConnection();
            connection2.connect();

            // subscribe and wait for the retain message to arrive
            connection2.subscribe(new Topic[]{new Topic("a/b/c", AT_LEAST_ONCE)});
            Thread.sleep(TIMEOUT * 2);
            result = connection2.receive(TIMEOUT, TimeUnit.MILLISECONDS);
            assertEquals(expectedResult, new String(result.getPayload(), "UTF-8"));
            result.ack();

        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        } finally {

            if (connection2 != null && connection2.isConnected()) {
                connection2.disconnect();
                while (connection2.isConnected()) {
                    Thread.sleep(TIMEOUT);
                }
            }

            // clear the retained message
            result = null;
            connection.publish("a/b/c", "".getBytes(), AT_LEAST_ONCE, true);
            Thread.sleep(TIMEOUT);
            result = connection.receive(TIMEOUT, TimeUnit.MILLISECONDS);
            assertNull(result);
        }
    }

    public void testLWT() throws Exception {

        // sub on client 1
        connection.subscribe(new Topic[]{new Topic(WILL_TOPIC, AT_MOST_ONCE)});

        expectedResult = WILL_MESSAGE;
        result = null;
        Thread.sleep(TIMEOUT);


        MQTT client2 = null;
        BlockingConnection connection2 = null;
        try {

            client2 = new MQTT();
            client2.setCleanSession(true);
            client2.setKeepAlive((short) CONNECTION_TIMEOUT);
            client2.setHost(BROKER_URL);
            client2.setClientId(CLIENT_ID_2);
            client2.setWillTopic(WILL_TOPIC);
            client2.setWillMessage(WILL_MESSAGE);
            connection2 = client2.blockingConnection();
            connection2.connect();
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }

        // wait for the client to connect and then kill the client
        Thread.sleep(CONNECTION_TIMEOUT * 2);
        connection2.kill();

        // sleep a bit more and verify we got the LWT on client 1
        Thread.sleep(CONNECTION_TIMEOUT * 15);
        result = connection.receive(TIMEOUT, TimeUnit.MILLISECONDS);
        assertEquals(expectedResult, new String(result.getPayload(), "UTF-8"));
        result.ack();

    }

    public void testKeepAlive() throws Exception {
        // fail("Not implemented");
    }

    public void connectionLost(Throwable cause) {
        System.err.println("Connection Lost");
    }


//	public void messageArrived(String topic, MqttMessage message)
//			throws Exception {
//		System.err.println("Message Arrived on " + topic + " with "
//				+ new String(message.getPayload()));
//		result = new String(message.getPayload());
//	}
//
//	public void deliveryComplete(IMqttDeliveryToken token) {
//		System.err.println("Delivery Complete: " + token.getMessageId());
//	}
}
