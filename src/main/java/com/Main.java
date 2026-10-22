package com;

import com.simplekafka.broker.BrokerInfo;
import com.simplekafka.broker.Protocol;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
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
        SocketChannel ch = SocketChannel.open(new InetSocketAddress("localhost", 9091));

        ByteBuffer req = Protocol.encodeCreateTopicRequest("pagamentos", 2, (short) 2);
        while (req.hasRemaining()) {
            ch.write(req);
        }

        ByteBuffer response = ByteBuffer.allocate(1024);
        ch.read(response);
        response.flip();

        byte type = response.get();
        if (type == Protocol.CREATE_TOPIC_RESPONSE) {
            System.out.println("Tópico criado!");
        } else {
            short len = response.getShort();
            byte[] bytes = new byte[len];
            response.get(bytes);
            System.out.println("Erro: " + new String(bytes, StandardCharsets.UTF_8));
        }

        // 1. monta e envia o pedido (só 1 byte: o tipo METADATA)
        ByteBuffer request = Protocol.encodeMetadataRequest();
        while (request.hasRemaining()) {
            ch.write(request);
        }

        // 2. lê a resposta
        ByteBuffer response2 = ByteBuffer.allocate(64 * 1024);
        ch.read(response2);
        response2.flip();

        // 3. decodifica com o seu decoder do Stage 2
        Protocol.MetadataResult result = Protocol.decodeMetadataResponse(response2);
        ch.close();

        if (!result.isSuccess()) {
            System.out.println("Erro: " + result.getError());
            return;
        }

        // 4. imprime
        for (BrokerInfo b : result.getBrokers()) {
            System.out.println(b);
        }

        for (Protocol.TopicMetadata t : result.getTopics()) {
            System.out.println("Tópico: " + t.getName());
            for (Protocol.PartitionMetadata p : t.getPartitions()) {
                System.out.println("  partição " + p.getId() + " → líder " + p.getLeader() + ", réplicas " + p.getReplicas());
            }
        }
        ch.close();
        }

    }