import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by user on 1/13/16.
 */
public class SumCountAverageTest {

    @Test
    public void testToString() throws Exception {
        SumCountAverage sca = new SumCountAverage(123,5);
        assertEquals("123,24.6", sca.toString());
    }

    @Test
    public void testSetDelimiter() throws Exception {
        SumCountAverage sca = new SumCountAverage(123,5);
        sca.setDelimiter("\t");
        assertEquals("123\t24.6", sca.toString());
    }
}