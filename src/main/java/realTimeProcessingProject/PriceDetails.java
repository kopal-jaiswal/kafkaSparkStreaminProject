package realTimeProcessingProject;
import java.io.Serializable;

public class PriceDetails implements Serializable {
	   private float close;
	    private float high;
	    private float low;
	    private float open;
	    private float volume;
	
	
	public void setClose(float close) {
        this.close= close;
    }

    public void setHigh(float high) {
        this.high= high;
    }

    public void setLow(float low) {
        this.low= low;
    }
    public void setOpen(float open) {
        this.open= open;
    }
    public void setVolume(float volume) {
        this.volume= volume;
    }

    public float getClose() {
        return close;
    }

    public float getHigh() {
        return high;
    }

    public float getLow() {
        return low;
    }
    public float getOpen() {
        return open;
    }
    public float getVolume() {
        return volume;
    }
	@Override
	public String toString() {
		return "PriceData {close=" + close + ", high=" + high + ", low=" + low+ ", open="+open+",volume="+volume+"}";
	}
	

}
