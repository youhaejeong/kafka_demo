package com.yhj.demo

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import java.util.Properties

fun main(){
    val props = Properties()
    props[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
    props[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] =
        "org.apache.kafka.common.serialization.StringSerializer"
    props[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] =
        "org.apache.kafka.common.serialization.StringSerializer"

    val producer = KafkaProducer<String,String>(props);

    repeat(10){i ->
        val record = ProducerRecord("test-topic","key-$i","message-$i-Hi");
        producer.send(record);
        println("sent: key-$i, message-$i , $i Hi")
    }
    producer.close()
}