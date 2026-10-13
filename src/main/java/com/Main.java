package com;

import com.simplekafka.broker.Partition;
import com.simplekafka.broker.ZookeeperClient;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {

    static void print(String label, List<byte[]> msgs) {
        List<String> texts = new ArrayList<>();
        for (byte[] m : msgs) texts.add(new String(m));
        System.out.println(label + " → " + texts);
    }

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        ZookeeperClient zk = new ZookeeperClient("localhost", 2181);
        zk.connect();
        System.out.println("Connected to Zookeeper");
        zk.createPersistentNode("/topics/pedidos", "{\"partitions\":2}");
        zk.createEphemeralNode("/brokers/ids/1", "{\"host\":\"localhost\",\"port\":9091}");
        System.out.println("Topic: " + zk.getData("/topics/pedidos"));
        System.out.println("Broker: " + zk.getData("/brokers/ids/1"));
        zk.watchChildren("/brokers/ids", children ->
                System.out.println(">>> Brokers vivos: " + children));
        zk.createEphemeralNode("/brokers/ids/1", "broker1");
        Thread.sleep(500);
        zk.createEphemeralNode("/brokers/ids/2", "broker2");

        zk.watchNode("/controller", () -> System.out.println(">>> Controller mudou!"));
        zk.createEphemeralNode("/controller", "{\"broker\":1}");
        zk.close();

        Partition p = new Partition(0, 1, List.of(), "data/pedidos-0");
        p.append("oi".getBytes());
        p.append("tchau".getBytes());
        p.append("kafka".getBytes());

        print("a partir do 0", p.readMessages(0, 1000));   // [oi, tchau, kafka]
        print("a partir do 1", p.readMessages(1, 1000));   // [tchau, kafka]
        print("maxBytes 6",    p.readMessages(0, 6));      // [oi]          (oi=2, +tchau=7 > 6)
        print("maxBytes 1",    p.readMessages(0, 1));      // [oi]          (a primeira sempre vem)
        print("offset 3",      p.readMessages(3, 1000));   // []            (ainda não existe)
        long offset = p.append(("execução às " + LocalTime.now()).getBytes());
        System.out.println("Gravei no offset " + offset);
        print("Tudo na partição", p.readMessages(0, 100000));
        p.close();
        System.out.println("Partição criada!");
        }

    }