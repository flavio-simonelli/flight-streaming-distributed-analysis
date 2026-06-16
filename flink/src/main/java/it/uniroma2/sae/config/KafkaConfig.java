package it.uniroma2.sae.config;

/**
 * Kafka-specific configuration properties.
 */
public class KafkaConfig {
    private String host;
    private int port;
    private String topic;
    private String groupId;

    public String getHost() { return host; }
    public void setHost(String host) { this.host = host; }

    public int getPort() { return port; }
    public void setPort(int port) { this.port = port; }

    public String getTopic() { return topic; }
    public void setTopic(String topic) { this.topic = topic; }

    public String getGroupId() { return groupId; }
    public void setGroupId(String groupId) { this.groupId = groupId; }
}
