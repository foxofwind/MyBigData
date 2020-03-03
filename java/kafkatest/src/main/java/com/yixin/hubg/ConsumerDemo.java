package com.yixin.hubg;

import java.util.Arrays;
import java.util.Properties;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.yixin.hubg.domain.FaApplyInfoExtSource;
import com.yixin.hubg.domain.KafkaData;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class ConsumerDemo {

    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "192.168.145.132:9091");
        properties.put("group.id", "group-1");
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("auto.offset.reset", "earliest");
        properties.put("session.timeout.ms", "30000");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(Arrays.asList("fa_apply_info_ext_hubg"));
        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                KafkaData<FaApplyInfoExtSource> sdata = JSON.parseObject(record.value()
                        , new TypeReference<KafkaData<FaApplyInfoExtSource>>() {});
                //KafkaData sdata = JSON.parseObject(record.value(), KafkaData.class);
//                System.out.println(sdata);
//                System.out.println(sdata.getTable());
//                System.out.println(sdata.getData().get(0));
//                System.out.println(sdata.getData().get(0).getDbDMLType());
                System.out.printf("offset = %d, key = %s, value = %s", record.offset(), record.key(),record.value());
                System.out.println("=====================>");
            }
        }

    }
}