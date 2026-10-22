package com.simplekafka.broker;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ZookeeperClient implements Watcher {

    private static final Logger LOGGER = Logger.getLogger(ZookeeperClient.class.getName());

    private static final int SESSION_TIMEOUT = 30000;

    private final String host;
    private final int port;

    private ZooKeeper zooKeeper;
    private CountDownLatch connectedSignal = new CountDownLatch(1);

    public ZookeeperClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    private String getConnectString() {
        return host + ":" + port;
    }

    public void connect() throws IOException, InterruptedException, KeeperException {

        zooKeeper = new ZooKeeper(getConnectString(), SESSION_TIMEOUT, this);
        connectedSignal.await();

        // Create required paths if they don't exist
        createPath("/brokers/ids");
        createPath("/topics");
    }

    public void createPath(String path) throws KeeperException, InterruptedException {

        String [] parts = path.split("/");
        String current = "";

        for (String part : parts) {
            if (part.isEmpty()) continue;
            current = current + "/" + part;
            if (zooKeeper.exists(current, false) == null) {
                zooKeeper.create(current, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        }
    }

    public String getData(String path) throws KeeperException, InterruptedException {

        if (zooKeeper.exists(path, false) == null) {
            return null;
        }
        byte[] data = zooKeeper.getData(path, false, null);
        return new String(data, StandardCharsets.UTF_8);
    }

    @Override
    public void process(WatchedEvent event) {

        if (event.getState() == Event.KeeperState.SyncConnected) {
            connectedSignal.countDown();
            LOGGER.info("Connected to ZooKeeper");
        } else if (event.getState() == Event.KeeperState.Disconnected) {
            LOGGER.warning("Disconnected from ZooKeeper");
        } else if (event.getState() == Event.KeeperState.Expired) {
            LOGGER.warning("ZooKeeper session expired, reconnecting...");
            try {
                if (zooKeeper != null) {
                    zooKeeper.close();
                }
                connectedSignal = new CountDownLatch(1);
                zooKeeper = new ZooKeeper(getConnectString(), SESSION_TIMEOUT, this);
                connectedSignal.await();
                LOGGER.info("Reconnected to ZooKeeper after session expiry");
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Failed to reconnect to ZooKeeper", e);
            }
        }
    }

    public void close() throws InterruptedException {
        if (zooKeeper != null) {
            zooKeeper.close();
        }
    }

    public void createPersistentNode(String path, String data) throws KeeperException, InterruptedException {
        Stat stat = zooKeeper.exists(path, false);
        if (stat == null) {
            zooKeeper.create(path, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            LOGGER.info("Created persistent node: " + path);
        } else {
            zooKeeper.setData(path, data.getBytes(), -1);
            LOGGER.info("Updated persistent node: " + path);
        }
    }

    public boolean createEphemeralNode(String path, String data) throws KeeperException, InterruptedException {
        Stat stat = zooKeeper.exists(path, false);
        if (stat == null) {
            zooKeeper.create(path, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            LOGGER.info("Created ephemeral node: " + path);
            return true;
        } else {
            LOGGER.info("Ephemeral node already exists: " + path);
            return false;
        }
    }

    public interface ChildrenCallback {

        void onChildrenChanged(List<String> children);
    }

    public interface NodeCallback {
        void onNodeChanged();
    }

    public void watchChildren(String path, ChildrenCallback callback) {

        try {
            List<String> children = zooKeeper.getChildren(path, event ->  {
                if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                    watchChildren(path, callback);
                }
            });
            callback.onChildrenChanged(children);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to watch children: " + path, e);
        }
    }

    public void watchNode(String path, NodeCallback callback) {

        try {
            zooKeeper.exists(path, event ->  {
                if (event.getType() == Event.EventType.NodeDeleted || event.getType() == Event.EventType.NodeDataChanged || event.getType() == Event.EventType.NodeCreated) {
                    watchNode(path, callback);
                    callback.onNodeChanged();
                }
            });
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to watch node: " + path, e);
        }
    }

    public boolean exists(String path) throws KeeperException, InterruptedException {

        return zooKeeper.exists(path, false) != null;
    }

    public void deleteNode(String path) throws KeeperException, InterruptedException {

        zooKeeper.delete(path, -1);
    }

    public List<String> getChildren(String path) throws  KeeperException, InterruptedException {

        return zooKeeper.getChildren(path, false);
    }
}
