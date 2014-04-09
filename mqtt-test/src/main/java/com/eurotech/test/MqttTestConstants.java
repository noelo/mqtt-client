/*
 * Copyright (c) 2013 Eurotech.
 */
package com.eurotech.test;

public interface MqttTestConstants {
    public static final String BROKER_URL = System.getProperty("broker.host", "tcp://192.168.2.10:1883");
    public static final String CLIENT_ID = "test-client-id";
    public static final String CLIENT_ID_2 = "test-client-id-2";
    public static final int TIMEOUT = 500;
    public static final int CONNECTION_TIMEOUT = 1000;
    public static final String TMP_DIR = System.getProperty("java.io.tmpdir");

    public static final String WILL_TOPIC = "l/w/t";
    public static final String WILL_MESSAGE = "This is an LWT";
}
