/*
 * Copyright 2014 Giannakouris - Salalidis Victor
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.gsvic.csmr;

import com.gsvic.csmr.io.DocumentWritable;
import com.gsvic.csmr.io.VectorArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.VectorWritable;

import java.io.IOException;
import java.util.ArrayList;

public class CSMRReducer extends Reducer<IntWritable, DocumentWritable, Text, VectorArrayWritable> {
    //~ Instance fields --------------------------------------------------------

    private final Text outputKey = new Text();

    private final VectorArrayWritable outputValue = new VectorArrayWritable();

    //~ Methods ----------------------------------------------------------------

    @Override
    public void reduce(IntWritable key, Iterable<DocumentWritable> values,
                       Context context) throws IOException, InterruptedException {

        ArrayList<DocumentWritable> vectorArrayList = new ArrayList<>();
        
        /* Storing each key-value pair (document) in a java.util.ArrayList */
        for (DocumentWritable v : values) {
            vectorArrayList.add(new DocumentWritable(v.getKey(), v.getValue()));
        }
        
        /* Generating all the possible combinations of documents */
        if (vectorArrayList.size() > 0) {
            for (int i = 0; i < vectorArrayList.size(); ++i) {
                for (int j = i + 1; j < vectorArrayList.size(); ++j) {
                    VectorWritable[] val = new VectorWritable[2];
                    
                    /* Generating the key for the current document pair with
                        the format "doci_name@docj_name" */
                    String k = vectorArrayList.get(i).getKey().toString() +
                            "@" + vectorArrayList.get(j).getKey().toString();
                    this.outputKey.set(k);

                    //First Document (doci)
                    val[0] = new VectorWritable(vectorArrayList.get(i).getValue().get());
                    //Second Document (docj)
                    val[1] = new VectorWritable(vectorArrayList.get(j).getValue().get());
                    this.outputValue.set(val);

                    context.write(this.outputKey, this.outputValue);
                }
            }
        }
    }

}

// End CSMRReducer.java
