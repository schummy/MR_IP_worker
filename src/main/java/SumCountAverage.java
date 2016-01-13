import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by user on 1/12/16.
 */
public class SumCountAverage implements Writable {
    protected long sum = 0;
    protected long count = 1;
    protected Logger logger;
    private String delimiter = ",";

    public SumCountAverage(long sum, long count) {
        this.count = count;
        this.sum = sum;
        setLogger(LoggerFactory.getLogger(SumCountAverage.class));
        //logger.info("SumCountAverage({}, {}) constructor call", sum, count);
    }

    public SumCountAverage() {
        setLogger(LoggerFactory.getLogger(SumCountAverage.class));
        //logger.info("SumCountAverage constructor call");
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        //logger.info("SumCountAverage write call");

        dataOutput.writeLong(sum);
        dataOutput.writeLong(count);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        //logger.info("SumCountAverage read call");
        setSum(dataInput.readLong());
        setCount(dataInput.readLong());
    }

    public String toString(){

        //logger.info("SumCountAverage toString call");
        return sum + delimiter + getAverage();
    }

    public void setSum(long sum) {
        this.sum = sum;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public long getSum() {
        return sum;
    }

    public long getCount() {
        return count;
    }
    public double getAverage() {
        return (double)sum/count;
    }

    public void setLogger(Logger logger) {
        this.logger = logger;
    }

    public String getDelimiter() {
        return delimiter;
    }

    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }

    public int hashCode() {
        int result = 17;
        result = 31 * result + (int)(sum ^ (sum >>> 32));
        result = 31 * result + (int)(count ^ (count >>> 32));
        return result;
    }
    public boolean equals(Object obj){
        if (obj == this)
            return true;
        if (!(obj instanceof SumCountAverage))
            return false;

        SumCountAverage scObj = (SumCountAverage) obj;
        return scObj.sum == sum && scObj.count==count;
    }
}
