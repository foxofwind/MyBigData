package com.yixin.hubg;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ProducerDemo {

    public static void main(String[] args){
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "192.168.145.133:9092");
        properties.put("acks", "all");   // 确认所有副本写入消息
        properties.put("retries", 0);  // 重试次数
        properties.put("batch.size", 16384);   // 批量发送的的缓存大小
        properties.put("linger.ms", 1);  //消息延迟发送的毫秒数,批次发送
        properties.put("buffer.memory", 33554432); // 队列内存
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = null;
        try {
            producer = new KafkaProducer<String, String>(properties);
            producer.send(new ProducerRecord<String, String>("hubg","1"
                    ,"Every single time you access a website,"));
            producer.send(new ProducerRecord<String, String>("hubg","2"
                    ,"you leave tracks. Tracks that others can access."));
            producer.send(new ProducerRecord<String, String>("hubg","3"
                    ,"If you don't like the idea,"));
            producer.send(new ProducerRecord<String, String>("hubg","4"
                    ,"find out what software can help you cover them."));
//            for (int i = 0; i < 1; i++) {
//                String msg = "This is Message ";// + i;
//                producer.send(new ProducerRecord<String, String>("streams-plaintext-input", msg));
//                System.out.println("Sent:" + msg);
//            }
        } catch (Exception e) {
            e.printStackTrace();

        } finally {
            producer.close();
        }

    }
}