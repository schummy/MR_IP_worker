import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by user on 1/12/16.
 */
public class SumCount implements Writable {
    protected long sum = 0;
    protected long count = 1;
    protected Logger logger;

    public SumCount(long sum, long count) {
        this.count = count;
        this.sum = sum;
        setLogger(LoggerFactory.getLogger(SumCount.class));
        logger.info("SumCount({}, {}) constructor call", sum, count);
    }

    public SumCount() {
        setLogger(LoggerFactory.getLogger(SumCount.class));
        logger.info("SumCount constructor call");
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
       // logger.info("SumCount write call");

        dataOutput.writeLong(sum);
        dataOutput.writeLong(count);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        //logger.info("SumCount read call");
        setSum(dataInput.readLong());
        setCount(dataInput.readLong());
    }

    public String toString(){

        logger.info("SumCount toString call");
        return new String("sum:"+sum +"; count:" + count+";");
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

    public void setLogger(Logger logger) {
        this.logger = logger;
    }
}
