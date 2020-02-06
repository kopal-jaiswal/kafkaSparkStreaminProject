package realTimeProcessingProject;

	import java.io.Serializable;
	import java.util.ArrayList;
	import java.util.Iterator;
	import java.util.List;

	import org.apache.spark.api.java.function.Function2;
	import org.apache.spark.api.java.function.PairFlatMapFunction;
	import org.apache.spark.streaming.api.java.JavaDStream;
	import org.apache.spark.streaming.api.java.JavaPairDStream;

	import scala.Tuple2;
public class Transformations implements Serializable{

		private static final long serialVersionUID = 1L;

		public void getMoveAvgClosingPrice(JavaPairDStream<String, StockAverage> result) {
			JavaPairDStream<String, String> pair =  result.flatMapToPair(new PairFlatMapFunction<Tuple2<String,StockAverage>,String,String>() {

				private static final long serialVersionUID = 1L;

				@Override
				public Iterator<Tuple2<String, String>> call(Tuple2<String, StockAverage> t) throws Exception {
					List<Tuple2<String, String>> list = new ArrayList<Tuple2<String, String>>();

					list.add(new Tuple2<String, String>(t._1, t._2.getAvgMovingClosingPrice()));
					return list.iterator();
				}
			});
			pair.print();
		}

		public void getMaxProfit(JavaPairDStream<String, StockAverage> result){

			// Getting the tuple having maximum stock profit
			JavaDStream<Tuple2<String, StockAverage>> result2 = result.reduce(new Function2<Tuple2<String,StockAverage>, Tuple2<String,StockAverage>, Tuple2<String,StockAverage>>() {

				private static final long serialVersionUID = 1L;

				@Override
				public Tuple2<String, StockAverage> call(Tuple2<String, StockAverage> v1, Tuple2<String, StockAverage> v2)
						throws Exception {
					if (v1._2().getProfit()  > v2._2().getProfit())
						return v1;

					return v2;
				}
			});

			JavaPairDStream<String, String> pair =  result2.flatMapToPair(new PairFlatMapFunction<Tuple2<String,StockAverage>,String,String>() {

				private static final long serialVersionUID = 1L;

				@Override
				public Iterator<Tuple2<String, String>> call(Tuple2<String, StockAverage> t) throws Exception {
					List<Tuple2<String, String>> list = new ArrayList<Tuple2<String, String>>();

					list.add(new Tuple2<String, String>(t._1, t._2.getMaxProfit()));
					return list.iterator();
				}
			});

			pair.print();
		}

		public void  getTradingVolume(JavaPairDStream<String, StockAverage> result){


			// Getting the tuple having maximum stock profit
			JavaDStream<Tuple2<String, StockAverage>> result2 = result.reduce(new Function2<Tuple2<String,StockAverage>, Tuple2<String,StockAverage>, Tuple2<String,StockAverage>>() {

				private static final long serialVersionUID = 1L;

				@Override
				public Tuple2<String, StockAverage> call(Tuple2<String, StockAverage> v1, Tuple2<String, StockAverage> v2)
						throws Exception {
					if (v1._2().getTradingVolume()  > v2._2().getTradingVolume())
						return v1;

					return v2;
				}
			});

			JavaPairDStream<String, String> pair =  result2.flatMapToPair(new PairFlatMapFunction<Tuple2<String,StockAverage>,String,String>() {

				private static final long serialVersionUID = 1L;

				@Override
				public Iterator<Tuple2<String, String>> call(Tuple2<String, StockAverage> t) throws Exception {
					List<Tuple2<String, String>> list = new ArrayList<Tuple2<String, String>>();

					list.add(new Tuple2<String, String>(t._1, t._2.getMaxTradingVolume()));
					return list.iterator();
				}
			});

			pair.print();
		}

	}




