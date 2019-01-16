/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.broaddata.common.util;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;
import java.util.Properties;
import javax.jms.JMSException;
import javax.jms.MessageListener;
import javax.jms.ObjectMessage;
import org.apache.activemq.pool.PooledConnection;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

/**
 *
 * @author fxy1949
 */
public class KafkaTest {
    
    public KafkaTest() {
    }
    
    @BeforeClass
    public static void setUpClass() {
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    @Before
    public void setUp() {
    }
    
    @After
    public void tearDown() {
    }

    /**
     * Test of getConnection method, of class JMSUtil.
     */
    @Ignore
    @Test
    public void testSendMesage() 
    {    
        KafkaProducer producer = KafkaUtil.getKafkaProducer("127.0.0.1");
        
        for(int i=0;i<1000;i++)
            KafkaUtil.sendMessage(producer, "video-frames", "key1", String.format("%d - 11111111111",i));
    }
    
    @Ignore
    @Test
    public void testConsumeMesage() 
    {    
   //Kafka consumer configuration settings
        String topicName = "testtopic";
        Properties props = new Properties();

        //多个ip用逗号分割
        props.put("bootstrap.servers", "127.0.0.1:9092");

        //group 代表一个消费组
        props.put("group.id", "test");
        //是否开启自动提交
        props.put("enable.auto.commit", "true");

        //zk连接超时
        props.put("zookeeper.session.timeout.ms", "400000");
        //zk follower落后于zk leader的最长时间
        props.put("zookeeper.sync.time.ms", "200");
        //自动检测提交间隔
        props.put("auto.commit.interval.ms", "1000");

        props.put("auto.offset.reset", "latest");
        //序列化类
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, Object> consumer = new KafkaConsumer<String, Object>(props);
        consumer.subscribe(Arrays.asList(topicName));//自动分摊partition给group中所有的consumer

        while (true) {

            ConsumerRecords<String, Object> records = consumer.poll(100);
            for (ConsumerRecord<String, Object> record : records){
                
                System.out.println(" key ="+record.key()+" value="+record.value());
       
                break;
            }
        }
        
    }
}
