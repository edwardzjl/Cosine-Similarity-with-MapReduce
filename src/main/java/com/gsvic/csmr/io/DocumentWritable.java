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

package com.gsvic.csmr.io;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by edwardlol on 2017/7/5.
 */
public class DocumentWritable implements Writable {
    //~ Instance fields --------------------------------------------------------

    private Text key;

    private VectorWritable value;

    //~ Constructors -----------------------------------------------------------

    public DocumentWritable() {
        this.key = new Text();
        this.value = new VectorWritable();
    }

    public DocumentWritable(Text key, VectorWritable value) {
        this.key = new Text(key);
        this.value = new VectorWritable(value.get());
    }

    public DocumentWritable(String key, Vector value) {
        this.key = new Text(key);
        this.value = new VectorWritable(value);
    }

    //~ Methods ----------------------------------------------------------------

    @Override
    public void readFields(DataInput in) throws IOException {
        this.key.readFields(in);
        this.value.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.key.write(out);
        this.value.write(out);
    }

    public Text getKey() {
        return this.key;
    }

    public VectorWritable getValue() {
        return this.value;
    }


    public void set(String key, Vector value) {
        if (this.key == null) {
            this.key = new Text();
        }
        if (this.value == null) {
            this.value = new VectorWritable();
        }
        this.key.set(key);
        this.value.set(value);
    }

    public void set(Text key, VectorWritable value) {
        if (this.key == null) {
            this.key = new Text();
        }
        if (this.value == null) {
            this.value = new VectorWritable();
        }
        this.key.set(key);
        this.value.set(value.get());
    }


    @Override
    public String toString() {
        return this.key.toString() + "\n" + this.value.toString();
    }
}

// End DocumentWritable.java
