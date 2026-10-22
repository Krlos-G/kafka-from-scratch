package com.simplekafka.broker;

import org.apache.zookeeper.KeeperException;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.simplekafka.broker.Protocol.CREATE_TOPIC;

public class SimpleKafkaBroker {

    private static final Logger LOGGER = Logger.getLogger(SimpleKafkaBroker.class.getName());
    private static final String DATA_DIR = "data";


    private final int brokerId;
    private final String brokerHost;
    private final int brokerPort;

    // tópico → (número da partição → Partition)    ex.: "pedidos" → {0 → Partition, 1 → Partition}
    private final Map<String, Map<Integer, Partition>> topics;
    // id do broker → BrokerInfo  (quem está no cluster, preenchido na parte 2)
    private final Map<Integer, BrokerInfo> clusterMetadata;

    private final ExecutorService executor;
    private final ServerSocketChannel serverChannel;
    private final AtomicBoolean isRunning;
    private final AtomicBoolean isController;
    private final ZookeeperClient zkClient;

    public SimpleKafkaBroker(int brokerId, String host, int port, int zkPort) throws IOException {

        this.brokerId = brokerId;
        this.brokerHost = host;
        this.brokerPort = port;
        this.topics = new ConcurrentHashMap<>();
        this.executor = Executors.newFixedThreadPool(10);
        this.serverChannel = ServerSocketChannel.open();
        this.isRunning = new AtomicBoolean(false);
        this.isController = new AtomicBoolean(false);
        this.clusterMetadata = new ConcurrentHashMap<>();
        // Inicia data directory
        File dataDir = new File(DATA_DIR + File.separator + brokerId);
        if (!dataDir.exists()) {
            dataDir.mkdirs();
        }
        // Inicia ZooKeeper client
        this.zkClient = new ZookeeperClient("localhost", zkPort);
    }

    public int getBrokerId() {
        return brokerId;
    }

    public String getBrokerHost() {
        return brokerHost;
    }

    public int getBrokerPort() {
        return brokerPort;
    }

    public Map<String, Map<Integer, Partition>> getTopics() {
        return topics;
    }

    public Map<Integer, BrokerInfo> getClusterMetadata() {
        return clusterMetadata;
    }

    public ExecutorService getExecutor() {
        return executor;
    }

    public ServerSocketChannel getServerChannel() {
        return serverChannel;
    }

    public AtomicBoolean getIsRunning() {
        return isRunning;
    }

    public AtomicBoolean getIsController() {
        return isController;
    }

    public ZookeeperClient getZkClient() {
        return zkClient;
    }

    public void start() throws Exception {

        serverChannel.bind(new InetSocketAddress(brokerHost, brokerPort));
        serverChannel.configureBlocking(false);
        isRunning.set(true);

        registerWithZookeeper();
        electController();
        zkClient.watchNode("/controller", this::onControllerChange);
        loadExistingTopics();

        new Thread(this::acceptConnections).start();
        LOGGER.info("Broker " + brokerId + " started on " + brokerHost + ":" + brokerPort);
    }

    public void stop() {

        isRunning.set(false);
        try {
            serverChannel.close();
            for (Map<Integer, Partition> partitions : topics.values()) {
                for (Partition p : partitions.values()) {
                    p.close();
                }
            }
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Error stopping broker  " + brokerId, e);
        }
            executor.shutdownNow();
        try {
            zkClient.close();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void acceptConnections() {

        while (isRunning.get()) {
            try {
                SocketChannel clientChannel = serverChannel.accept();
                if (clientChannel != null) {
                    clientChannel.configureBlocking(false);

                    // Handle client connection in a separate thread
                    executor.submit(() -> handleClient(clientChannel));
                }
                Thread.sleep(100); // Small pause to prevent CPU spin
            } catch (Exception e) {
                if (isRunning.get()) {
                    LOGGER.log(Level.SEVERE, "Error accepting connection", e);
                }
            }
        }
    }
    private void handleClient(SocketChannel clientChannel) {
        try {
            ByteBuffer buffer = ByteBuffer.allocate(1024);
            while (clientChannel.isOpen() && isRunning.get()) {
                buffer.clear();
                int bytesRead = clientChannel.read(buffer);
                if (bytesRead > 0) {
                    buffer.flip();
                    // Process the message based on protocol
                    processClientMessage(clientChannel, buffer);
                } else if (bytesRead < 0) {
                    // Connection closed by client
                    clientChannel.close();
                    break;
                }
                Thread.sleep(50); // Small pause to prevent CPU spin
            }
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Error handling client", e);
        } finally {
            try {
                if (clientChannel.isOpen()) {
                    clientChannel.close();
                }
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, "Error closing client channel", e);
            }
        }
    }
    private void processClientMessage(SocketChannel clientChannel, ByteBuffer buffer) throws IOException {
        byte messageType = buffer.get();
        switch (messageType) {
            case Protocol.PRODUCE:
                handleProduceRequest(clientChannel, buffer);
                break;
            case Protocol.FETCH:
                handleFetchRequest(clientChannel, buffer);
                break;
            case Protocol.METADATA:
                handleMetadataRequest(clientChannel, buffer);
                break;
            case CREATE_TOPIC:
                handleCreateTopicRequest(clientChannel, buffer);
                break;
            case Protocol.REPLICATE:
                handleReplicateRequest(clientChannel, buffer);
                break;
            case Protocol.TOPIC_NOTIFICATION:
                handleTopicNotification(clientChannel, buffer);
                break;
            default:
                LOGGER.warning("Unknown message type: " + messageType);
                Protocol.sendErrorResponse(clientChannel, "Unknown message type");
        }
    }

    private void handleProduceRequest(SocketChannel clientChannel, ByteBuffer buffer) throws IOException {

        Protocol.sendErrorResponse(clientChannel, "PRODUCE not implemented yet");
    }

    private void handleFetchRequest(SocketChannel clientChannel, ByteBuffer buffer) throws IOException {

        Protocol.sendErrorResponse(clientChannel, "FETCH not implemented yet");
    }

    private void handleMetadataRequest(SocketChannel clientChannel, ByteBuffer buffer) throws IOException {

        ByteBuffer response = ByteBuffer.allocate(64 * 1024);
        response.put(Protocol.METADATA_RESPONSE);
        response.putInt(clusterMetadata.size());
        for (BrokerInfo b : clusterMetadata.values()) {
            byte [] hostBytes = b.getHost().getBytes(StandardCharsets.UTF_8);
            response.putInt(b.getId());
            response.putShort((short) hostBytes.length);
            response.put(hostBytes);
            response.putInt(b.getPort());
        }
        try  {
            List<String>  topicNames = zkClient.getChildren("/topics");
            response.putInt(topicNames.size());
            for (String topic: topicNames) {
                byte [] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
                response.putShort((short) topicBytes.length);
                response.put(topicBytes);
                List<String> partitionsIds =  zkClient.getChildren("/topics/" + topic  + "/partitions");
                response.putInt(partitionsIds.size());
                for (String pid : partitionsIds) {
                    int partId = Integer.parseInt(pid);
                    String state = zkClient.getData("/topics/" + topic + "/partitions/" + pid + "/state");
                    String [] parts = state.split(":");
                    int leader = Integer.parseInt(parts[0]);
                    List<Integer> replicas = new ArrayList<>();
                    for (String r : parts[1].split(",")) replicas.add(Integer.parseInt(r));
                    response.putInt(partId);
                    response.putInt(leader);
                    response.putInt(replicas.size());
                    for (int r : replicas) response.putInt(r);

                }
            }
        } catch (Exception e) {
            Protocol.sendErrorResponse(clientChannel, "Failed  to read metadata: " +  e.getMessage());
            return;
        }
        response.flip();
        while (response.hasRemaining()){
            clientChannel.write(response);
        }
    }

    private void handleCreateTopicRequest(SocketChannel clientChannel, ByteBuffer buffer) throws IOException {

        try {
            short len = buffer.getShort();
            byte [] bytes = new byte[len];
            buffer.get(bytes);
            String topic = new String(bytes, StandardCharsets.UTF_8);
            int numPartitions = buffer.getInt();
            short replicationFactor = buffer.getShort();

            if (!topic.matches("[a-zA-Z0-9._-]+")) {
                Protocol.sendErrorResponse(clientChannel, "Invalid topic name");
                return;
            }
            if (!isController.get()) {
                Protocol.sendErrorResponse(clientChannel, "Not the controller");
                return;
            }
            if (zkClient.exists("/topics/" + topic)) {
                Protocol.sendErrorResponse(clientChannel, "Topic already exists");
                return;
            }
            if (replicationFactor > clusterMetadata.size()) {
                Protocol.sendErrorResponse(clientChannel, "Not enough brokers for replication factor");
                return;
            }

            createTopic(topic, numPartitions, replicationFactor);
            ByteBuffer buf = ByteBuffer.allocate(1);
            buf.put(Protocol.CREATE_TOPIC_RESPONSE);
            buf.flip();
            while (buf.hasRemaining()) {
                clientChannel.write(buf);
            }
        } catch (Exception e) {
            Protocol.sendErrorResponse(clientChannel, "Failed to create a topic: " + e.getMessage());
        }
    }

    public void createTopic(String topic, int numPartitions, short replicationFactor) throws Exception {

        List<Integer> ids = new ArrayList<>(clusterMetadata.keySet());
        Collections.sort(ids);

        zkClient.createPersistentNode("/topics/" + topic, numPartitions + ":" + replicationFactor);

        for (int p = 0; p < numPartitions; p++) {
            int leader = ids.get(p % ids.size());
            List<Integer> replicas = new ArrayList<>();
            for (int r = 0; r < replicationFactor; r++) {
                replicas.add(ids.get((p + r) % ids.size()));
            }
            String state = leader + ":" + String.join(",", replicas.stream().map(String::valueOf).toList());
            String partitionPath = "/topics/" + topic + "/partitions/" + p;
            zkClient.createPath(partitionPath);
            zkClient.createPersistentNode(partitionPath + "/state", state);
        }
        loadTopic(topic);
        notifyBrokers(topic);
        LOGGER.info("Created topic " + topic + " with " + numPartitions + " partitions");
    }

    public void loadTopic(String topic) throws Exception {

        if (topics.containsKey(topic)) {
            return;
        }
        Map<Integer, Partition> localPartitions = new ConcurrentHashMap<>();
        List<String> partitionsIds = zkClient.getChildren("/topics/" + topic + "/partitions");
        for (String pid : partitionsIds) {
            int partitionId = Integer.parseInt(pid);
            String state = zkClient.getData("/topics/" + topic + "/partitions/" + pid + "/state");
            String [] parts = state.split(":");
            int leader = Integer.parseInt(parts[0]);
            List<Integer> replicas = new ArrayList<>();
            for (String r : parts[1].split(",")) replicas.add(Integer.parseInt(r));

            if (replicas.contains(brokerId)) {

                List<Integer> followers = new ArrayList<>(replicas);
                followers.remove(Integer.valueOf(leader));
                String dir = DATA_DIR + File.separator + brokerId + File.separator + topic + "-" + partitionId;
                localPartitions.put(partitionId, new Partition(partitionId, leader, followers, dir));
            }
        }
        topics.put(topic, localPartitions);
        LOGGER.info("Loading topic " + topic + " Local partitions: " + localPartitions.keySet());
    }

    private void loadExistingTopics() throws Exception {

        List<String> topicNames = zkClient.getChildren("/topics");
        for (String topic : topicNames) {
            try {
                loadTopic(topic);
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "Failed to load topic " + topic, e);
            }
        }
        LOGGER.info("Loaded " + topicNames.size() + " existing  topics");
    }

    private void handleReplicateRequest(SocketChannel clientChannel, ByteBuffer buffer) throws IOException {

        Protocol.sendErrorResponse(clientChannel, "REPLICATE not implemented yet");
    }

    private void handleTopicNotification(SocketChannel clientChannel, ByteBuffer buffer) throws IOException {

        try   {
            short len = buffer.getShort();
            byte [] bytes  = new byte[len];
            buffer.get(bytes);
            String topic = new String (bytes, StandardCharsets.UTF_8);
            loadTopic(topic);
        } catch (Exception e){
            LOGGER.log(Level.WARNING, "Failed to load topic notification", e);
        }
    }


    private void onBrokersChanged(List<String> brokersIds){
        try {
            for (String idBrok : brokersIds) {
                int id = Integer.parseInt(idBrok);
                if (!clusterMetadata.containsKey(id)) {
                    String data = zkClient.getData("/brokers/ids/" + id);
                    if (data != null) {
                        String [] parts = data.split(":");
                        clusterMetadata.put(id, new BrokerInfo(id, parts[0], Integer.parseInt(parts[1])));
                    }
                }
            }
            clusterMetadata.keySet().removeIf(id -> !brokersIds.contains(String.valueOf(id)));
            LOGGER.info("Brokers in cluster: " + clusterMetadata.keySet());
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Error handling broker change", e);
        }
    }

    private void electController(){

        try {
            String controllerPath = "/controller";

            // Check if node exists and is valid
            boolean nodeExists = zkClient.exists(controllerPath);
            if (nodeExists) {
                String existingData = zkClient.getData(controllerPath);
                if (existingData == null || existingData.trim().isEmpty()) {
                    zkClient.deleteNode(controllerPath);
                    nodeExists = false;
                }
            }
            // Try to become controller
            boolean becameController = false;
            if (!nodeExists) {
                becameController = zkClient.createEphemeralNode(controllerPath, String.valueOf(brokerId));
            }
            if (becameController) {
                isController.set(true);
                LOGGER.info("Broker " + brokerId + " is now the CONTROLLER");
            } else {
                // Watch for controller changes
                LOGGER.info("Broker " + brokerId + " is not the controller");
            }
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Controller election failed", e);
            // Retry election after delay
            new Thread(() -> {
                try {
                    Thread.sleep(2000);
                    electController();
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }).start();
        }
    }

    private void onControllerChange() {
        try {
            if (!zkClient.exists("/controller")) {
                electController();
            }
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Error handling controller change", e);
        }
    }

    private void registerWithZookeeper() throws Exception {

        zkClient.connect();
        zkClient.createEphemeralNode("/brokers/ids/" + brokerId, brokerHost + ":" + brokerPort);
        clusterMetadata.put(brokerId, new BrokerInfo(brokerId, brokerHost, brokerPort));
        zkClient.watchChildren("/brokers/ids", this::onBrokersChanged);
    }

    private void  notifyBrokers(String topic) {

        for  (BrokerInfo b : clusterMetadata.values()) {
            if  (b.getId() == brokerId) {
                continue;
                }
            try (SocketChannel ch = SocketChannel.open(new InetSocketAddress(b.getHost(), b.getPort()))) {
                ByteBuffer  msg  = Protocol.encodeTopicNotification(topic);
                while (msg.hasRemaining())
                    ch.write(msg);
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, "Failed  to notify  broker "  +  b.getId() +   " about topic "  + topic, e);
            }
            }
    }


    public static void main(String[] args) throws  Exception {
        int brokerId = Integer.parseInt(args[0]);
        int port = Integer.parseInt(args[1]);
        int zkPort = Integer.parseInt(args[2]);
        SimpleKafkaBroker broker = new SimpleKafkaBroker(brokerId, "localhost", port, zkPort);
        broker.start();
        Runtime.getRuntime().addShutdownHook(new Thread(broker::stop));
    }
}