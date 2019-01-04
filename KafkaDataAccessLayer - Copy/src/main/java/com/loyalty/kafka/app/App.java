package com.loyalty.kafka.app;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.loyalty.kafka.constants.IKafkaConstants;
import com.loyalty.kafka.couchbase.CBQuery;
import com.loyalty.kafka.cli.ConsumerCreator;
import com.loyalty.kafka.cli.ProducerCreator;

public class App {
	public  static CBQuery cb_obj = null;
	
	// Logger instance
	private static final Logger logger = LoggerFactory
			.getLogger(App.class);
    public static void main(String[] args) {
    	//cb_obj = new CBQuery();
    	int sequenceId = 1;
    	cb_obj = new CBQuery(logger);
    	runKafkaTranscations();    	
    }
   
    
    private static void runKafkaTranscations() {    	
    	  	
    	//Step 1 : create Consumer
    	Consumer<String, String> consumer = ConsumerCreator.createConsumer();
		consumer.subscribe(Collections.singletonList("profilelinkingservice"));
				
		//Step 2: create Producer 
		//We need producer to send back the results after process.
		Producer<String, String> producer = getTransactionProducer();
		producer.initTransactions();
		System.out.println("KAFKA : initiated ");

		while (true) {
			// Step 3: Read
			ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
			if(!records.isEmpty())
			for (ConsumerRecord<String, String> record : records) 
	        {
				producer.beginTransaction();

				System.out.println("KAFKA : record " + record);

	        	//parse the document_id/rewards id
				try{
					JSONObject document = new  JSONObject(record.value());
							//.fromJson(record.value());
					
		    		System.out.println("Docuemnt ater fromJson is :" + document);
		    		String document_id = null;
		    		String insert_document_id = null;
		    		JSONObject member_json  = document.getJSONArray("memberId").getJSONObject(0);
		    		
		    		System.out.println("member_id :" + member_json);
		    		System.out.println("member_id typeCode :" + member_json.get("typeCode"));
		    		
		    		long sendOffsetsResult = record.offset();
//		    		TopicPartition partition = record.;
		    		
		    		if(member_json.get("typeCode").equals("CSID")) {
		    			document_id = (String) member_json.get("value");
		    			System.out.println("Docuemnt ID : "  +document_id);
		    		}
	//	    			//call couchbase
		    		try{
		        	if( document_id != null)
		        		//fixed this insert with deb & anshul's suggestion.
		        		// deb & Anshul: if the document id exists then update else create
		        		insert_document_id = cb_obj.insert(document_id, record.value().toString());
		    		}catch(NullPointerException e){
		    			System.out.println("KAFKA: document exists");
		    			insert_document_id = null;
		    		}
		        	if( insert_document_id != null )
		        	{
		        		producer.send(new ProducerRecord<String,String>("profilelinkingservice-return", insert_document_id ));
			        	System.out.print("KAFKA Transactions record is : sent ");
	
		        	} else {
		        		producer.send(new ProducerRecord<String,String>("profilelinkingservice-return", "Document exists" ));
			        	System.out.print("KAFKA Transactions Document Exists");

		        	}
		        	document_id = null;
		        	insert_document_id = null;
				}catch (JSONException e){
					logger.error(e.getLocalizedMessage());
				}
	        	
	        }	  
	        // To ensure that the consumed and produced messages are batched, we need to commit
	        // the offsets through
	        // the producer and not the consumer.
	        //
	        // If this returns an error, we should abort the transaction.
//

		
//	    String sendOffsetsResult = producer.sendOffsetsToTransaction( getUncommittedOffests());	
		producer.commitTransaction();	

//	      Without transactions, you normally use Consumer#commitSync() or Consumer#commitAsync() to commit consumer offsets. But if you use these methods before you've produced with your producer, you will have committed offsets before knowing whether the producer succeeded sending.
//
//	      So, instead of committing your offsets with the consumer, you can use Producer#sendOffsetsToTransaction() on the producer to commit the offsets instead. This sends the offsets to the transaction manager handling the transaction. It will commit the offsets only if the entire transactions—consuming and producing—succeeds.
		}
    }

		


	private static Producer<String, String> getTransactionProducer() {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("transactional.id", "my-transactional-id-1");
		System.out.println("KAFKA : before creating producer ");		
		return new KafkaProducer<String,String>(props, new StringSerializer(), new StringSerializer());
	}

  
}
