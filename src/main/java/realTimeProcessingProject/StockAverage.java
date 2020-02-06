package realTimeProcessingProject;
import java.io.Serializable;
public class StockAverage implements Serializable {
		/**
		 *
		 */
		private static final long serialVersionUID = 332323;
		private int count;
		private double closePrice;
		private double profit;
		private double tradingVolume;
		
		
		public StockAverage(int count, double closePrice, double profit, double tradingVolume) {
			super();
			this.count = count;
			this.closePrice = closePrice;
			this.profit = profit;
			this.tradingVolume = Math.abs(tradingVolume);
		}
		
		public int getCount() {
			return count;
		}
		public void setCount(int count) {
			this.count = count;
		}

		public double getProfit() {
			return profit;
		}
		public void setProfit(double profit) {
			this.profit = profit;
		}

		public double getClosePrice() {
			return closePrice;
		}

		public void setClosePrice(double closePrice) {
			this.closePrice = closePrice;
		}

		public double getTradingVolume() {
			return tradingVolume;
		}

		public void setTradingVolume(double tradingVolume) {
			this.tradingVolume = tradingVolume;
		}
		
		public String getAvgMovingClosingPrice() {
			return "MaxTradingVolume ---> "  + String.valueOf(closePrice/count);
		}
		
		public String getMaxProfit() {
			return "MaxProfit ---> "  +  String.valueOf(this.getProfit());
		}
		
		public String getMaxTradingVolume() {
			return "MaxTradingVolume ---> "  +  String.valueOf(this.getTradingVolume());
		}
		

}
