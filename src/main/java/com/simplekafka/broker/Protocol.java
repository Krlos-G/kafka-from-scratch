package com.simplekafka.broker;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Protocol {

    // Client request types
    public static final byte PRODUCE = 0x01;
    public static final byte FETCH = 0x02;
    public static final byte METADATA = 0x03;
    public static final byte CREATE_TOPIC = 0x04;
    public static final byte REPLICATE = 0x05;
    public static final byte TOPIC_NOTIFICATION = 0x06;

    // Broker response types
    public static final byte PRODUCE_RESPONSE = 0x11;
    public static final byte FETCH_RESPONSE = 0x12;
    public static final byte METADATA_RESPONSE = 0x13;
    public static final byte ERROR_RESPONSE = 0x14;
    public static final byte CREATE_TOPIC_RESPONSE = 0x15;

    public static ByteBuffer encodeProduceRequest(String topic, int partition, byte[] message){
        // layout: [1 tipo][2 tam. tópico][N tópico][4 partição][4 tam. msg][M msg]
        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);

        ByteBuffer buf = ByteBuffer.allocate(1 + 2 + topicBytes.length + 4 + 4 +message.length);
        buf.put(PRODUCE); // tipo
        buf.putShort((short) topicBytes.length); // tamanho do tópico
        buf.put(topicBytes); // tópico
        buf.putInt(partition); // partição
        buf.putInt(message.length); // tamanho da mensagem
        buf.put(message); // mensagem
        buf.flip();
        return buf;
    }

    public static ByteBuffer encodeFetchRequest(String topic, int partition, long offset, int maxBytes){

        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);

        ByteBuffer buf = ByteBuffer.allocate(1 + 2 + topicBytes.length + 4 + 8 + 4);
        buf.put(FETCH);
        buf.putShort((short) topicBytes.length);
        buf.put(topicBytes);
        buf.putInt(partition);
        buf.putLong(offset);
        buf.putInt(maxBytes);
        buf.flip();
        return buf;
    }

    public static ByteBuffer encodeMetadataRequest(){

        ByteBuffer buf = ByteBuffer.allocate(1);
        buf.put(METADATA);
        buf.flip();
        return buf;
    }

    public static ByteBuffer encodeCreateTopicRequest(String topic, int numPartitions, short replicationFactor){

        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);

        ByteBuffer buf = ByteBuffer.allocate(1 + 2 + topicBytes.length + 4 + 2);
        buf.put(CREATE_TOPIC);
        buf.putShort((short) topicBytes.length);
        buf.put(topicBytes);
        buf.putInt(numPartitions);
        buf.putShort(replicationFactor);
        buf.flip();
        return buf;
    }

    public static ByteBuffer encodeReplicateRequest(String topic, int partition, long offset, byte[] message){

        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);

        ByteBuffer buf = ByteBuffer.allocate(1 + 2 + topicBytes.length + 4 + 8 + 4 + message.length);
        buf.put(REPLICATE);
        buf.putShort((short) topicBytes.length);
        buf.put(topicBytes);
        buf.putInt(partition);
        buf.putLong(offset);
        buf.putInt(message.length);
        buf.put(message);
        buf.flip();
        return buf;
    }

    public static ByteBuffer encodeTopicNotification(String topic){

        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buf = ByteBuffer.allocate(1 + 2 + topicBytes.length);
        buf.put(TOPIC_NOTIFICATION);
        buf.putShort((short) topicBytes.length);
        buf.put(topicBytes);
        buf.flip();
        return buf;
    }

    public static ProduceResult decodeProduceResponse(ByteBuffer buf){

        byte type = buf.get();
        if (type == ERROR_RESPONSE) {
            short len = buf.getShort();
            byte [] bytes = new byte[len];
            buf.get(bytes);
            String text = new String(bytes, StandardCharsets.UTF_8);
            return new ProduceResult(-1, text);
        }
        if (type != PRODUCE_RESPONSE){
            return  new ProduceResult(-1, "Unexpected response type: " + type);
        }
        long offset = buf.getLong();
        return new ProduceResult(offset, null);

    }

    public static class ProduceResult {

        private final long offset;
        private final String error;

        public ProduceResult(long offset, String error) {
            this.offset = offset;
            this.error = error;

        }

        public long getOffset() {
            return offset;
        }

        public String getError() {
            return error;
        }

        public boolean isSuccess() {
            return error == null;
        }
    }

    public static FetchResult decodeFetchResponse(ByteBuffer buf){

        byte type = buf.get();
        if (type == ERROR_RESPONSE) {
            short len = buf.getShort();
            byte [] bytes = new byte[len];
            buf.get(bytes);
            String text = new String(bytes, StandardCharsets.UTF_8);
            return new FetchResult(new ArrayList<>(), text);
        }
        if (type != FETCH_RESPONSE){
            return  new FetchResult(new ArrayList<>(), "Unexpected response type: " + type);
        }

        int count = buf.getInt();
        List<byte[]> messages = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            long offset = buf.getLong();
            int len = buf.getInt();
            byte[] bytes = new byte[len];
            buf.get(bytes);
            messages.add(bytes);
        }
        return new FetchResult(messages, null);
    }

    public static class FetchResult {

        private final List<byte[]> messages;
        private final String error;
        public FetchResult(List<byte[]> messages, String error) {
            this.messages = messages;
            this.error = error;
        }

        public List<byte[]> getMessages() {
            return messages;
        }

        public String getError() {
            return error;
        }

        public boolean isSuccess() {
            return error == null;
        }
    }

    public static MetadataResult decodeMetadataResponse(ByteBuffer buf){

        byte type = buf.get();
        if (type == ERROR_RESPONSE) {
            short len = buf.getShort();
            byte [] bytes = new byte[len];
            buf.get(bytes);
            String text = new String(bytes, StandardCharsets.UTF_8);
            return new MetadataResult(new ArrayList<>(), new ArrayList<>(), text);
        }
        if (type != METADATA_RESPONSE) {
            return   new MetadataResult(new ArrayList<>(), new ArrayList<>(), "Unexpected response type: " + type);
        }

        int brokerCount = buf.getInt();
        List<BrokerInfo> brokers = new ArrayList<>();
        for (int i = 0; i < brokerCount; i++){
            int brokerId = buf.getInt();
            short hostLen = buf.getShort();
            byte[] hostBytes = new byte[hostLen];
            buf.get(hostBytes);
            String host = new String(hostBytes, StandardCharsets.UTF_8);
            int brokerPort = buf.getInt();
            brokers.add(new BrokerInfo(brokerId, host, brokerPort));
        }

        int topicCount = buf.getInt();
        List<TopicMetadata> topics = new ArrayList<>();
        for (int i = 0; i < topicCount; i++) {
            short topicLen = buf.getShort();
            byte[] topicBytes = new byte[topicLen];
            buf.get(topicBytes);
            String topic = new String(topicBytes, StandardCharsets.UTF_8);

            int partitionCount = buf.getInt();
            List<PartitionMetadata> partitions = new ArrayList<>();
            for (int j = 0; j < partitionCount; j++) {
                int partitionId = buf.getInt();
                int leaderId = buf.getInt();

                int replicaCount = buf.getInt();
                List<Integer> replicas = new ArrayList<>();
                for (int k = 0; k < replicaCount; k++) {
                    replicas.add(buf.getInt());
                }
                partitions.add(new PartitionMetadata(partitionId, leaderId, replicas));
            }
            topics.add(new TopicMetadata(topic, partitions));
        }
        return new MetadataResult(brokers, topics, null);
    }

    public static class PartitionMetadata {

        private final int id;
        private final int leader;
        private final List<Integer> replicas;

        public PartitionMetadata(int id, int leader, List<Integer> replicas) {
           this.id = id;
           this.leader = leader;
           this.replicas = replicas;
        }

        public int getId() {
            return id;
        }

        public int getLeader() {
            return leader;
        }

        public List<Integer> getReplicas() {
            return replicas;
        }
    }

    public static class TopicMetadata {

        private final String name;
        private final List<PartitionMetadata> partitions;

        public TopicMetadata(String name, List<PartitionMetadata> partitions) {
            this.name = name;
            this.partitions = partitions;
        }

        public String getName() {
            return name;
        }

        public List<PartitionMetadata> getPartitions() {
            return partitions;
        }
    }

    public static class MetadataResult {

        private final List<BrokerInfo> brokers;
        private final List<TopicMetadata> topics;
        private final String error;

        public MetadataResult(List<BrokerInfo> brokers, List<TopicMetadata> topics, String error) {
            this.brokers = brokers;
            this.topics = topics;
            this.error = error;
        }

        public List<BrokerInfo> getBrokers() {
            return brokers;
        }

        public List<TopicMetadata> getTopics() {
            return topics;
        }

        public String getError() {
            return error;
        }

        public boolean isSuccess() {
            return error == null;
        }
    }

    public static void sendErrorResponse(SocketChannel channel, String errorMessage) throws  IOException {

        byte [] errorBytes = errorMessage.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buf = ByteBuffer.allocate(1 + 2 + errorBytes.length);
        buf.put(ERROR_RESPONSE);
        buf.putShort((short) errorBytes.length);
        buf.put(errorBytes);
        buf.flip();
        while (buf.hasRemaining()) {
            channel.write(buf);
        }
    }
}
