/*
 * Copyright (c) 2013 Eurotech.
 */
package com.eurotech.test;

//import org.fusesource.mqtt.client.BlockingConnection;
//import org.fusesource.mqtt.client.MQTT;
//
//public class IdleMqttClient implements MqttTestConstants {
//	// private static final MqttDefaultFilePersistence DATA_STORE = new
//	// MqttDefaultFilePersistence(TMP_DIR);
//
//	private static MQTT client;
//
//	public static void main(String[] args) {
//		try {
//			client = new MQTT();
//			// MqttConnectOptions options = new MqttConnectOptions();
//			client.setCleanSession(true);
//			client.setKeepAlive((short) (CONNECTION_TIMEOUT * 5 / 1000));
//			client.setWillTopic(WILL_TOPIC);
//			client.setConnectAttemptsMax(0);// , WILL_MESSAGE.getBytes(), 0,
//											// false);
//			// client = new MqttClient(BROKER_URL, "idle-mqtt-client",
//			// DATA_STORE);
//			client.setHost(BROKER_URL);
//			BlockingConnection connection = client.blockingConnection();
//			connection.connect();
//
//			Thread.sleep(60000);
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//	}
//}
