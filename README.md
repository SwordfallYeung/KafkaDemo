# KafkaDemo
Apache kafka是消息中间件的一种， 用于构建实时数据管道和流应用程序，小白学习

kafka中文教程：<br/>
http://orchome.com/kafka/index<br/>
<br/>
Kafka在windows安装运行：<br/>
http://blog.csdn.net/evankaka/article/details/52421314<br/>
<br/>
如何为Kafka集群选择合适的partitions数量：<br/>
https://www.cnblogs.com/fanguangdexiaoyuer/p/6066820.html<br/>
Apache Kafka 分布式消息队列中间件安装与配置：<br/>
http://blog.csdn.net/wangjia184/article/details/37921183<br/>

# 注意：
1. kafka安装目录下config/server.properties有如下配置信息，listeners配置的是hostname：<br/>
> listeners=PLAINTEXT://node1:9092

利用kafka的producer API生产数据时，需要在程序运行的客户端的hosts文件上配置kafka集群每台机器的ip-hostname，如：<br/>
>192.168.187.201 node1<br/>
 192.168.187.202 node2<br/>
 192.168.187.203 node3
 
 否则会报错<br/>
 - 配置log4j日志，可以看到报 "java.io.IOException: Can't resolve address: node1:9092"错误
 - 不配置日志，可以看到报"Expiring 3 record(s) for zlikun_topic-1: 30043 ms has passed since batch creation plus linger time"错误
 
2. kafka安装目录下config/server.properties有如下配置信息，listeners配置的是ip地址：<br/>
 > listeners=PLAINTEXT://192.168.187.201:9092
 
 参考资料：<br/>
 配hostname的解决方法：https://segmentfault.com/q/1010000008863969<br/>
 直接配ip地址，而不是hostname: http://www.orchome.com/946
 
 kafka安装部署请参考：https://www.cnblogs.com/swordfall/p/8859053.html
