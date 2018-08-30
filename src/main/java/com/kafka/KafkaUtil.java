package com.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

/**
 * @Author: Yang JianQiu
 * @Date: 2018/8/28 19:38
 */
public class KafkaUtil {

    private static KafkaConsumer<String, String> consumer;
    private static KafkaProducer<String, String> producer;

    public static void main(String[] args) {
        //createMessageToKafka();

        consumMessageFromKafka();
    }

    /**
     * 生产者产生数据并写入到kafka中
     */
    public static void createMessageToKafka(){

        Properties props = new Properties();

        //Kafka集群连接串，可以由多个host:port组成
        props.put("bootstrap.servers", "192.168.187.201:9092,192.168.187.201:9092,192.168.187.201:9092");

        //broker消息确认的模式，有三种：默认1
//        0：不进行消息接收确认，即Client端发送完成后不会等待Broker的确认
//        1：由Leader确认，Leader接收到消息后会立即返回确认信息
//        all：集群完整确认，Leader会等待所有in-sync的follower节点都确认收到消息后，再返回确认信息
        props.put("acks", "all");

        //发送失败时Producer端的重试次数，默认为0
        props.put("retries", 0);

        //当同时有大量消息要向同一个分区发送时，Producer端会将消息打包后进行批量发送。如果设置为0，则每条消息都独立发送。默认为16384字节
        props.put("batch.size", 16384);

        //发送消息前等待的毫秒数，与batch.size配合使用。在消息负载不高的情况下，配置linger.ms能够让Producer在发送消息前等待一定时间，以积累更多的消息打包发送，达到节省网络资源的目的。默认为0
        props.put("linger.ms", 1);

        //消息缓冲池大小。尚未被发送的消息会保存在Producer的内存中，如果消息产生的速度大于消息发送的速度，那么缓冲池满后发送消息的请求会被阻塞。默认33554432字节（32MB）
        props.put("buffer.memory", 33554432);

        //消息key/value的序列器Class，根据key和value的类型决定
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // 分区接口类
        //props.put("partitioner.class","");

        //K代表每条消息的key类型，V代表消息类型。消息的key用于决定此条消息由哪一个partition接收，所以我们需要保证每条消息的key是不同的。
        producer = new KafkaProducer<String, String>(props);

        try {
            while (true){
                String imsiMessage = randomPhone() + "," + randomDevice() + "," + getTime();
                String faceMessage = randomImage() + "," + randomDevice() + "," + getTime();
                producer.send(new ProducerRecord<String, String>("phone", imsiMessage), new Callback() {
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e != null)
                            System.out.println(e.getMessage());
                        System.out.println("message send to partition " + recordMetadata.partition() + ",");
                    }
                });
                producer.send(new ProducerRecord<String, String>("image", faceMessage), new Callback() {
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e != null)
                            System.out.println(e.getMessage());
                        System.out.println("message send to partition " + recordMetadata.partition() + ",");
                    }
                });
                System.out.println("发送数据成功");

                Thread.sleep(2000);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        producer.close();
    }

    /**
     * 消费者消费kafka中的数据
     */
    public static void consumMessageFromKafka(){

        Properties props = new Properties();

        //kafka集群连接串，可以由多个host:port组成
        props.put("bootstrap.servers", "192.168.187.201:9092,192.168.187.202:9092,192.168.187.203:9092");

        //Consumer的group id，同一个group下的多个Consumer不会拉取到重复的消息，不同group下的Consumer则会保证拉取到每一条消息。注意，同一个group下的consumer数量不能超过分区数。必须要使用别的组名称， 如果生产者和消费者都在同一组，则不能访问同一组内的topic数据
        props.put("group.id", "test");

        //是否自动提交已拉取消息的offset。提交offset即视为该消息已经成功被消费，该组下的Consumer无法再拉取到该消息（除非手动修改offset）。默认为true
        props.put("enable.auto.commit", "false");

        props.put("auto.offset.reset", "earliest");

        //自动提交offset的间隔毫秒数，默认5000。
        //本 例中采用的是自动提交offset，Kafka client会启动一个线程定期将offset提交至broker。假设在自动提交的间隔内发生故障（比如整个JVM进程死掉），那么有一部分消息是会被 重复消费的。要避免这一问题，可使用手动提交offset的方式。构造consumer时将enable.auto.commit设为false，并在代 码中用consumer.commitSync()来手动提交。
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        consumer = new KafkaConsumer<String, String>(props);

        // 方法一:系统去调度,自动处理消息
        consumer.subscribe(Arrays.asList("phone"));

        while (true){
            //poll方法即是从Broker拉取消息，在poll之前首先要用subscribe方法订阅一个Topic
            //如 果Topic有多个partition，KafkaConsumer会在多个partition间以轮询方式实现负载均衡。如果启动了多个 Consumer线程，Kafka也能够通过zookeeper实现多个Consumer间的调度，保证同一组下的Consumer不会重复消费消息。注 意，Consumer数量不能超过partition数，超出部分的Consumer无法拉取到任何数据。
            ConsumerRecords<String, String> records = consumer.poll(100);//拉取超时毫秒数，如果没有新的消息可供拉取，consumer会等待指定的毫秒数，到达超时时间后会直接返回一个空的结果集
            for (ConsumerRecord<String, String> record : records) {

                System.out.println("fetched from partition " + record.partition() + ", offset: " + record.offset() + ",key:" + record.key() + ", message: " + record.value());
            }

            //提交已经拉取出来的offset,如果是手动模式下面,必须拉取之后提交,否则以后会拉取重复消息
            consumer.commitSync();

            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            //break;
        }
    }



    public static String randomPhone(){
        String s = "";
        Random random = new Random();
        for (int i = 0; i < 7; i++){
            int m = (int)random.nextInt(10);
            s = s + m;
        }
        return s;
    }

    public static String randomImage(){
        String s = "";
        Random random = new Random();
        for (int i = 0; i < 2; i++){
            int m = (int)random.nextInt(10);
            s = s + m;
        }
        return s;
    }

    public static String randomDevice(){
        String s = "";
        Random random = new Random();
        for (int i = 0; i < 7; i++){
            int m = (int)random.nextInt(10);
            s = s + m;
        }
        return s;
    }

    public static long getTime(){
        Date date = new Date();
        return date.getTime();
    }
}
