package io.iot.pulsar.mqtt.endpoint;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class MqttEndpointProperties {
    /**
     * If CleanSession is set to 0, the Server MUST resume communications with the Client based on state from the
     * current Session (as identified by the Client identifier). If there is no Session associated with the Client
     * identifier the Server MUST create a new Session. The Client and Server MUST store the Session after the Client
     * and Server are disconnected [MQTT-3.1.2-4]. After the disconnection of a Session that had CleanSession set to 0,
     * the Server MUST store further QoS 1 and QoS 2 messages that match any subscriptions that the client had at the
     * time of disconnection as part of the Session state [MQTT-3.1.2-5]. It MAY also store QoS 0 messages that meet
     * the same criteria.
     * <p>
     * If CleanSession is set to 1, the Client and Server MUST discard any previous Session and start a new one.
     * This Session lasts as long as the Network Connection. State data associated with this Session MUST NOT be
     * reused in any subsequent Session [MQTT-3.1.2-6].
     * <p>
     * <p>
     * The Session state in the Server consists of:
     * ·         The existence of a Session, even if the rest of the Session state is empty.
     * ·         The Client’s subscriptions.
     * ·         QoS 1 and QoS 2 messages which have been sent to the Client, but have not been completely acknowledged.
     * ·         QoS 1 and QoS 2 messages pending transmission to the Client.
     * ·         QoS 2 messages which have been received from the Client, but have not been completely acknowledged.
     * ·         Optionally, QoS 0 messages pending transmission to the Client.
     * <p>
     * Retained messages do not form part of the Session state in the Server, they MUST NOT be deleted when the
     * Session ends [MQTT-3.1.2.7].
     */
    @Builder.Default
    private boolean cleanSession = true;
}
