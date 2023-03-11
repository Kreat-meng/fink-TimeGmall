package com.atguigu.utils;


import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import java.util.Properties;

/**
 * @author MengX
 * @create 2023/3/8 19:48:27
 */
public class MyKafkaUtil {

    private static final String BOOTSTRAP_SERVERS = "hadoop102:9092,hadoop103:9092,hadoop102:9092";

    public static FlinkKafkaConsumer<String> getFlinkKafkaConsumer(String topic,String groupId){

        Properties props= new Properties();

        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVERS);

        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);

        FlinkKafkaConsumer<String> flinkKafkaConsumer = new FlinkKafkaConsumer<>(topic, new KafkaDeserializationSchema<String>() {
            @Override
            public boolean isEndOfStream(String nextElement) {
                return false;
            }

            @Override
            public String deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {

                if (record == null || record.value() == null) {

                    return null;
                } else

                    return new String(record.value());

            }

            @Override
            public TypeInformation<String> getProducedType() {

                return BasicTypeInfo.STRING_TYPE_INFO;
            }
        }, props);

        return flinkKafkaConsumer;
    }

    public static FlinkKafkaProducer<String> getKafkaProducerFunction(String topic){

        return new FlinkKafkaProducer<String>(
                BOOTSTRAP_SERVERS,topic,new SimpleStringSchema());

    }
}
