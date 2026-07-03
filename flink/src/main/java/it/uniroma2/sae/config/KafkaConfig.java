package it.uniroma2.sae.config;

import java.util.Map;

/**
 * Kafka-specific configuration properties.
 */
public class KafkaConfig {
    private String host;
    private Integer port;
    private Integer internalPort;
    private Integer externalPort;
    private String inputTopic;
    private String sinkTopic;
    private String groupId;
    private Map<String, String> outputTopics;

    public String getHost() { return host; }
    public void setHost(String host) { this.host = host; }

    public int getPort() {
        if (port != null) return port;
        if (internalPort != null) return internalPort;
        if (externalPort != null) return externalPort;
        return 9092; // Safe default Kafka broker port fallback
    }
    public void setPort(int port) { this.port = port; }

    public String getBootstrapServers() {
        return getHost() + ":" + getPort();
    }

    public Integer getInternalPort() { return internalPort; }
    public void setInternalPort(Integer internalPort) { this.internalPort = internalPort; }

    public Integer getExternalPort() { return externalPort; }
    public void setExternalPort(Integer externalPort) { this.externalPort = externalPort; }

    public String getInputTopic() { return inputTopic; }
    public void setInputTopic(String inputTopic) { this.inputTopic = inputTopic; }

    public String getSinkTopic() { return sinkTopic; }
    public void setSinkTopic(String sinkTopic) { this.sinkTopic = sinkTopic; }

    public String getGroupId() { return groupId; }
    public void setGroupId(String groupId) { this.groupId = groupId; }

    public Map<String, String> getOutputTopics() { return outputTopics; }
    public void setOutputTopics(Map<String, String> outputTopics) { this.outputTopics = outputTopics; }

    public String getOutputTopic(int query) { return getOutputTopic("q" + query); }
    public String getOutputTopic(String query) { return outputTopics.get(query); }
}
