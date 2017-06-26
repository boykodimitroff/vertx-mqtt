package eu.dreamix.controllers;

import io.vertx.mqtt.messages.MqttPublishMessage;

/**
 * @author Boyko Dimitrov on 6/26/17.
 */
public class LightsController {

    private static final String TURN_ON_INDICATOR = "1";
    private static final String TURN_OFF_INDICATOR = "0";

    public static void handler(MqttPublishMessage message) {
        if(TURN_ON_INDICATOR.equals(message.payload().toString())) {
            turnOn();
        } else if(TURN_OFF_INDICATOR.equalsIgnoreCase(message.payload().toString())) {
            turnOff();
        }
    }

    private static void turnOn() {
        System.out.println("Lights are on!");
    }

    private static void turnOff() {
        System.out.println("Lights are off!");
    }

}
