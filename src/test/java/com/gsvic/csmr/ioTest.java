package com.gsvic.csmr;

import com.gsvic.csmr.io.InputData;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;

/**
 * Created by edwardlol on 2017/7/6.
 */
public class ioTest {
    //~ Static fields/initializers ---------------------------------------------

    //~ Instance fields --------------------------------------------------------

    //~ Constructors -----------------------------------------------------------

    //~ Methods ----------------------------------------------------------------

    @Test
    public void getClassTests() {
        try {
            Text text = InputData.deepCopy(new Text("aaa"));
            System.out.println(text);

            IntWritable intWritable = InputData.deepCopy(new IntWritable(3));
            System.out.println(intWritable.get());

            LongWritable longWritable = InputData.deepCopy(new LongWritable(1L));
            System.out.println(longWritable.get());

            DoubleWritable doubleWritable = InputData.deepCopy(new DoubleWritable(2.0d));
            System.out.println(doubleWritable.get());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}

// End com.gsvic.csmr.ioTest.java
