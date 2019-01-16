/*
 * JMSUtil.java
 *
 */

package com.broaddata.common.util;

import org.apache.log4j.Logger;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
 
public class KafkaUtil 
{
    static final Logger log = Logger.getLogger("KafkaUtil");
    
    public static KafkaProducer getKafkaProducer(String kafkaServerIp)
    {
        Properties properties = new Properties();
        //此处配置的是kafka的端口
        properties.put("bootstrap.servers", String.format("%s:9092",kafkaServerIp));
        properties.put("acks", "all");
        properties.put("retries", 3); // 消息发送请求失败重试次数
        properties.put("batch.size", 2000);
        properties.put("linger.ms", 100); // 消息逗留在缓冲区的时间，等待更多的消息进入缓冲区一起发送，减少请求发送次数
        properties.put("buffer.memory", 335544320); // 内存缓冲区的总量
        // 如果发送到不同分区，并且不想采用默认的Utils.abs(key.hashCode) % numPartitions分区方式，则需要自己自定义分区逻辑
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
   
        return producer;
    }
    
    public static void sendMessage(KafkaProducer producer,String topic,String key,String value)
    {     
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);
        producer.send(record);
    }
    
    public static void closeProducer(KafkaProducer producer)
    {
        producer.close();
    }
    
}
