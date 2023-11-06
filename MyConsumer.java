package kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;
import java.util.Arrays;

public class MyConsumer {

	public static void main(String[] args){
		
		Properties consumerProps = new Properties();
		consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, ProducerConstantConfig.appID);
		consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ProducerConstantConfig.bootstrapServerList);
		consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
		consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "CONSUMER_GROUP1");
		consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		
		
		//Step 2. Create object for kafka consumer.
		KafkaConsumer<Integer,String> consumer = new KafkaConsumer<Integer,String>(consumerProps);
		
		consumer.subscribe(Arrays.asList("all_orders"));
		
		
		//Creating producer here
		//Step 3 Set the properties for Producer.
		Properties props = new Properties();
				
		props.put(ProducerConfig.CLIENT_ID_CONFIG, ConstantConfig.appID);
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ConstantConfig.bootstrapServerList);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
				
				
		//Step 4. Create object for kafka Producer. 
		KafkaProducer<Integer,String> producer = new KafkaProducer<Integer,String>(props);
		
		while(true){
			ConsumerRecords<Integer,String> records = consumer.poll(100);
			
			for(ConsumerRecord<Integer,String> record: records){
				
				if(record.value().split(",")[3].equals("CLOSED")){
					producer.send(new ProducerRecord<Integer,String>("closed_orders",record.key(),record.value()));
				}
				
				else{
					producer.send(new ProducerRecord<Integer,String>("completed_orders",record.key(),record.value()));
				}
				
			}
			
		}
		
	}
	
}
