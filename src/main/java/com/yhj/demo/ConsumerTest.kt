package com.yhj.demo

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.time.Duration
import java.util.Properties

fun main(){
    var props = Properties();
    props[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
    props[ConsumerConfig.GROUP_ID_CONFIG] = "test-group"
    props[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] =
        "org.apache.kafka.common.serialization.StringDeserializer"
    props[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] =
        "org.apache.kafka.common.serialization.StringDeserializer"
    props[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"

    val consumer = KafkaConsumer<String,String>(props)
    consumer.subscribe(listOf("test-topic"))

    while(true){
        val records= consumer.poll(Duration.ofMillis(100))

        for(record in records){
            println("received : key=${record.key()},value=${record.value()}, partition=${record.partition()}, offset=${record.offset()}")

        }
    }

}