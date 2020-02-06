package realTimeProcessingProject;
import java.io.Serializable;
import java.util.Date;

public class StockDetails implements Serializable {

	private static final long serialVersionUID = 1L;
    private Cryptotype symbol;
    private String timestamp;
    private PriceDetails priceData;
 
    
    public StockDetails(){
    }
    public void setSymbol(Cryptotype symbol) {
        this.symbol= symbol;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp= timestamp;
    }

    public void setPriceData(PriceDetails priceData) {
        this.priceData= priceData;
    }
    public Cryptotype getSymbol() {
        return symbol;
    }
        public String getTimestamp() {
            return timestamp;
        }

        public PriceDetails getPriceData() {
            return priceData;
        }
     @Override
    public String toString() {
        return "StockDetails [symbol=" + symbol + ", timestamp=" + timestamp + ", "+ priceData.toString() + "]";
    }

}
