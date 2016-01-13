import com.google.common.collect.ImmutableList;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by user on 1/13/16.
 */
public class IPWorkerTest {
    MapDriver<LongWritable, Text, Text, SumCountAverage> mapDriver;
    ReduceDriver<Text, SumCountAverage, Text, SumCountAverage> reduceDriver;
    MapReduceDriver<LongWritable, Text, Text, SumCountAverage, Text, SumCountAverage> mapReduceDriver;

    @Before
    public void setUp() throws Exception {
        IPWorker.IPMapper mapper = new IPWorker.IPMapper();
        IPWorker.IPReducer reducer = new IPWorker.IPReducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapperNotZeroSize() throws IOException {
        mapDriver.withInput(new LongWritable(), new Text(
                "ip1592 - - [28/Apr/2011:00:33:54 -0400] \"GET /sun_ss10/ss10_interior.jpg HTTP/1.1\" 200 12343 \"http://host2/sun_ss10/\" \"Mozilla/5.0 (Windows NT 6.1; WOW64; rv:2.0) Gecko/20100101 Firefox/4.0\""));
        mapDriver.withOutput(new Text("ip1592"), new SumCountAverage(12343, 1));
        mapDriver.runTest();
    }

    @Test
    public void testMapperZeroSize() throws IOException {
        mapDriver.withInput(new LongWritable(), new Text(
                "ip1594 - - [28/Apr/2011:00:33:54 -0400] \"GET /sun_ss10/ss10_interior.jpg HTTP/1.1\" 200 - \"http://host2/sun_ss10/\" \"Mozilla/5.0 (Windows NT 6.1; WOW64; rv:2.0) Gecko/20100101 Firefox/4.0\""));
        mapDriver.withOutput(new Text("ip1594"), new SumCountAverage(0, 1));
        mapDriver.runTest();
    }

    @Test
    public void testMapperWrongSize() throws IOException {
        mapDriver.withInput(new LongWritable(), new Text(
                "ip1594 - - [28/Apr/2011:00:33:54 -0400] \"GET /sun_ss10/ss10_interior.jpg HTTP/1.1\" 200 sdfsdfsd \"http://host2/sun_ss10/\" \"Mozilla/5.0 (Windows NT 6.1; WOW64; rv:2.0) Gecko/20100101 Firefox/4.0\""));
        mapDriver.withOutput(new Text("ip1594"), new SumCountAverage(0, 1));
        mapDriver.runTest();
    }

    @Test
    public void testReducer() throws IOException {
        reduceDriver.withInput(new Text("ip1"), ImmutableList.of(new SumCountAverage(123, 3), new SumCountAverage(23, 4)));
        reduceDriver.withOutput(new Text("ip1"), new SumCountAverage(146, 7));
        reduceDriver.runTest();

    }

    @Test
    public void testMapReducer() throws IOException {
        mapReduceDriver.withInput(new LongWritable(), new Text(
                "ip1592 - - [28/Apr/2011:00:33:54 -0400] \"GET /sun_ss10/ss10_interior.jpg HTTP/1.1\" 200 12343 \"http://host2/sun_ss10/\" \"Mozilla/5.0 (Windows NT 6.1; WOW64; rv:2.0) Gecko/20100101 Firefox/4.0\""));
        mapReduceDriver.withOutput(new Text("ip1592"), new SumCountAverage(12343, 1));
        mapReduceDriver.runTest();
    }
}