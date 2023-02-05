package com.whh.gmall.realtime.util;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Properties;

public class KafkaUtil {
    static String BOOTSTRAP_SERVERS = "hadoop102:9092,hadoop103:9092,hadoop104:9092";
    static String DEFAULT_topic = "default_topic";
    public static FlinkKafkaConsumer<String> getKafkaConsumer(String topic,String groupId){
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers",BOOTSTRAP_SERVERS);
        prop.setProperty("ConsumerConfig.GROUP_ID_CONFIG",groupId);
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(topic,
                new KafkaDeserializationSchema<String>() {
                    @Override
                    public TypeInformation<String> getProducedType() {
                        return TypeInformation.of(String.class);
                    }

                    @Override
                    public boolean isEndOfStream(String nextElement) {
                        return false;
                    }

                    @Override
                    public String deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
                        if (record != null && record.value() != null) {
                            return new String(record.value());
                        }
                        return null;
                    }
                }, prop);
        return consumer;
    }
}
