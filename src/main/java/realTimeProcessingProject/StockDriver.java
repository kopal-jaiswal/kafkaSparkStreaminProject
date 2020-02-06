package realTimeProcessingProject;


		import java.io.IOException;
		import java.util.ArrayList;
		import java.util.Arrays;
		import java.util.Date;
		import java.util.HashMap;
		import java.util.HashSet;
		import java.util.Iterator;
		import java.util.List;
		import java.util.Map;
		import java.util.Set;

		import org.apache.kafka.clients.consumer.ConsumerRecord;
		import org.apache.kafka.common.serialization.StringDeserializer;
		import org.apache.spark.SparkConf;
		import org.apache.spark.api.java.function.Function;
		import org.apache.spark.api.java.function.Function2;
		import org.apache.spark.api.java.function.PairFlatMapFunction;
		import org.apache.spark.streaming.Durations;
		import org.apache.spark.streaming.api.java.JavaDStream;
		import org.apache.spark.streaming.api.java.JavaInputDStream;
		import org.apache.spark.streaming.api.java.JavaPairDStream;
		import org.apache.spark.streaming.api.java.JavaStreamingContext;
		import org.apache.spark.streaming.kafka010.ConsumerStrategies;
		import org.apache.spark.streaming.kafka010.KafkaUtils;
		import org.apache.spark.streaming.kafka010.LocationStrategies;

		import com.fasterxml.jackson.core.type.TypeReference;
		import com.fasterxml.jackson.databind.ObjectMapper;
	
		import scala.Tuple2;
		
public final class StockDriver {

			public static void main(String[] args) throws Exception {


				if(args.length!=3) return;
				String bootstrapServers = args[2];
				String topicName = args[0];
				String groupId = args[1];
				

				SparkConf conf = new SparkConf().setAppName("StockAnalysis");
				conf.set("spark.testing.memory", "2147480000");
				JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.minutes(1));
				jssc.sparkContext().setLogLevel("WARN");

				Set<String> topicsSet = new HashSet<>(Arrays.asList(topicName.split(",")));
				Map<String, Object> props = new HashMap<>();
				props.put("bootstrap.servers", bootstrapServers);
				props.put("group.id", groupId);
				props.put("enable.auto.commit", "true");
				//props.put("auto.offset.reset", "earliest");
				props.put("key.deserializer", StringDeserializer.class);
				props.put("value.deserializer", StringDeserializer.class);



				// Subscribe to kafka topic
				final JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(
						jssc,
						LocationStrategies.PreferConsistent(),
						ConsumerStrategies.<String,String>Subscribe(topicsSet, props));



				//Convert to DStreams
				JavaDStream<StockDetails> jds =  messages.map(new Function<ConsumerRecord<String,String>, StockDetails>() {
					private static final long serialVersionUID = 1L;

					@Override
					public StockDetails call(ConsumerRecord<String, String> record) throws Exception {
						ObjectMapper mapper = new ObjectMapper();

						TypeReference<StockDetails> mapType = new TypeReference<StockDetails>() {};

						// Parsing the JSON String
						StockDetails stock = null;
						try {
							stock = mapper.readValue(record.value(), mapType);
						} catch (IOException e) {
							e.printStackTrace();
						}

						return stock;
					}
				}).cache();

				// Print the raw input
				jds.print();



				JavaPairDStream<String, StockAverage> pair = jds.flatMapToPair(
						new PairFlatMapFunction<StockDetails, String, StockAverage>() {
							private static final long serialVersionUID = 67676744;
							public Iterator<Tuple2<String, StockAverage>> call(StockDetails st)
									throws Exception {
								List<Tuple2<String, StockAverage>> list = new ArrayList<Tuple2<String, StockAverage>>();

								String symbol = st.getSymbol().toString();
								list.add(new Tuple2<String, StockAverage>(symbol,
										new StockAverage(1,
												st.getPriceData().getClose(), st.getPriceData().getClose() - st.getPriceData().getOpen(), st.getPriceData().getVolume())));
								return list.iterator();
							}
						}).cache();

				JavaPairDStream<String, StockAverage> result=pair.reduceByKeyAndWindow(
						new Function2<StockAverage, StockAverage, StockAverage>() {
							private static final long serialVersionUID = 76761212;
							public StockAverage call(StockAverage st1, StockAverage st2)
									throws Exception {
								st1.setClosePrice(
										st1.getClosePrice() + st2.getClosePrice());
								st1.setCount(st1.getCount() + st2.getCount());
								st1.setProfit(
										st1.getProfit() + st2.getProfit());
								st1.setTradingVolume(
										st1.getTradingVolume() + st2.getTradingVolume());

								return st1;
							}
						}, Durations.minutes(5), Durations.minutes(1)).cache();


				// Execute the  queries
				Transformations tfm= new Transformations();
				tfm.getMoveAvgClosingPrice(result);
				tfm.getMaxProfit(result);
				tfm.getTradingVolume(result);
				jssc.start();
				jssc.awaitTermination();
			}
}
	
