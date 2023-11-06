package kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;

public class MyProducer {
	
	public static void main(String[] args){
		
		//Step 1. Set the properties.
		Properties props = new Properties();
		
		props.put(ProducerConfig.CLIENT_ID_CONFIG, ConstantConfig.appID);
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ConstantConfig.bootstrapServerList);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		
		//Step 2. Create object for kafka Producer.
		KafkaProducer<Integer,String> producer = new KafkaProducer<Integer,String>(props);
		
		
		//Step 3. Calling the send method on this producer object.
		/*
		for(int i =1; i<=50; i++){
		producer.send(new ProducerRecord<Integer,String>(ConstantConfig.topicName,i,"This is my message number "+i));
		}
		*/
		
		producer.send(new ProducerRecord<Integer,String>(ConstantConfig.topicName,1,"1,2013-07-25 00:00:00.0,11599,CLOSED"));
		
		
		//Step 4.  Close the producer object.
		producer.close();
	}

}
