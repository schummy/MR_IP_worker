/**
 * Created by user on 1/12/16.
 */
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by user on 1/12/16.
 */
public class SumAvg extends SumCount {

    public SumAvg(long sum, long count) {
        this.count = count;
        this.sum = sum;
        setLogger(LoggerFactory.getLogger(SumAvg.class));
        logger.info("SumAvg({}, {}) constructor call", sum, count);
    }

    public SumAvg() {
        setLogger(LoggerFactory.getLogger(SumAvg.class));
        logger.info("SumAvg constructor call");
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
         logger.info("SumAvg write call");

        dataOutput.writeLong(sum);
        dataOutput.writeDouble((double) sum / count);
    }
    public String toString(){

        logger.info("SumAvg toString call");
        return new String("sum:"+sum +"; avg:" + (double) sum/count+";");
    }

}

