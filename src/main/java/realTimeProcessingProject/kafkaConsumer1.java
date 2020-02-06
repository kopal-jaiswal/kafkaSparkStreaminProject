package realTimeProcessingProject;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import scala.Tuple2;

public class kafkaConsumer1 {
//	static Map<String,Object> props= new HashMap<>();
	@SuppressWarnings("serial")
	public static void main(String[] args) throws InterruptedException {
		
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("FirstSparkApplication");
		conf.set("spark.testing.memory", "2147480000");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
		Logger.getRootLogger().setLevel(Level.ERROR);
		Properties props=new Properties();
		props.put("bootstrap.servers", "34.206.133.62:9092");
		props.put("group.id", "test5");
		props.put("enable.auto.commit", "true");
//		props.put(“auto.offset.reset”, “earliest”);
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
		consumer.subscribe(Arrays.asList("stockData"));
//		Collection<String> consumer2=Arrays.asList("stockData");
		while (true) {
			ConsumerRecords<String, String> recs = consumer.poll(Duration.ofMillis(100));
			for (ConsumerRecord<String, String> record : recs)
				{System.out.println("-------------consumer data--------------------");
			System.out.printf("offset = %d, key = %s, value = %s", record.offset(), record.key(),
			record.value());
				}
//		final JavaInputDStream<ConsumerRecord<String,String>> records= KafkaUtils.createDirectStream(jssc, LocationStrategies.PreferConsistent(),ConsumerStrategies.<String,String>Subscribe(consumer2,props));
//		
//		System.out.println("---------DSTREAMS-------------------------------");
//		records.mapToPair(new PairFunction<ConsumerRecord<String,String>, String, String>(){
//
//			public Tuple2<String, String> call(ConsumerRecord<String, String> record) throws Exception {
//				// TODO Auto-generated method stub
////				for (ConsumerRecord<String, String> record : t)
//				{System.out.printf("offset = %d, key = %s, value = %s", record.offset(), record.key(),
//						record.value());
//				return new Tuple2<>(record.key(),record.value());}
//			}
//		});
	}
	}}

