/*
 * Copyright 2015 Giannakouris - Salalidis Victor
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.mahout.common.StringTuple;
import org.apache.mahout.math.VectorWritable;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.HashMap;

/**
 * Created by edwardlol on 2017/7/5.
 */
public class InputData {
    //~ Methods ----------------------------------------------------------------

    @SuppressWarnings("unchecked cast")
    public static <T extends Writable> T deepCopy(T oldWritable) throws Exception {
        // TODO: 2017/7/6 why this can only be "<?>"?
        // why can't it be "<T>"
        Class<?> writableClazz = oldWritable.getClass();

        try {
            // like new Text(Text)
            Constructor<?> pConstructor = writableClazz.getConstructor(writableClazz);
            return (T) pConstructor.newInstance(oldWritable);
        } catch (NoSuchMethodException nsme) {
            // otherwise like newWritable.set(oldWritable.get());
            Constructor<?> noPConstructor = writableClazz.getConstructor();
            T newWritable = (T) noPConstructor.newInstance();

            Method getMethod = writableClazz.getMethod("get");
            Object content = getMethod.invoke(oldWritable);

            Method[] methods = writableClazz.getMethods();
            for (Method method : methods) {
                if (method.getName().equals("set") && method.getParameterCount() == 1) {
                    method.invoke(newWritable, content);
                    break;
                }
            }
            return newWritable;
        }
    }

    public static <K extends Writable, V extends Writable> HashMap<K, V> read(
            Configuration conf, Path file, Class<K> keyClass, Class<V> valueClass)
            throws Exception {

        SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(file));
        HashMap<K, V> content = new HashMap<>();

        K _key = keyClass.newInstance();
        V _value = valueClass.newInstance();

        while (reader.next(_key, _value)) {
            K key = deepCopy(_key);
            V value = deepCopy(_value);
            content.put(key, value);
        }
        return content;
    }

    /**
     * Reads a Hadoop Sequence File
     *
     * @param conf
     * @param input
     * @return Returns the given sequence file in a HashMap
     * @throws IOException
     * @deprecated
     */
    @Deprecated
    public static HashMap<Text, Text> readSequenceFile(Configuration conf, Path input)
            throws IOException {

        SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(input));

        HashMap<Text, Text> dcf = new HashMap<>();
        Text key = new Text();
        Text value = new Text();

        while (reader.next(key, value)) {
            dcf.put(new Text(key), new Text(value));
        }
        return dcf;
    }

    /**
     * Reads a Vectorized Text File, tfidf vectors/tf vectors
     *
     * @param conf
     * @param input
     * @return Returns the vectorized text file in a HashMap
     * @throws IOException
     * @deprecated
     */
    @Deprecated
    public static HashMap<Text, VectorWritable> vectorizedTextReader(Configuration conf, Path input)
            throws IOException {

        SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(input));

        HashMap<Text, VectorWritable> dcf = new HashMap<>();
        Text key = new Text();
        VectorWritable value = new VectorWritable();

        while (reader.next(key, value)) {
            dcf.put(new Text(key), new VectorWritable(value.get()));
        }
        return dcf;
    }

    /**
     * Reads the Document-Frequency file
     *
     * @param conf
     * @param dfFile
     * @return Returns the Document-Frequency data in a HashMap
     * @throws IOException
     * @deprecated
     */
    @Deprecated
    public static HashMap<IntWritable, LongWritable> readDf(Configuration conf, Path dfFile)
            throws IOException {

        SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(dfFile));

        HashMap<IntWritable, LongWritable> dcf = new HashMap<>();
        IntWritable key = new IntWritable();
        LongWritable value = new LongWritable();

        while (reader.next(key, value)) {
            dcf.put(new IntWritable(key.get()), new LongWritable(value.get()));
        }
        return dcf;
    }

    /**
     * Reads the dictionary file
     *
     * @param conf
     * @param dict
     * @return returns the dictionary in a HashMap
     * @throws IOException
     * @deprecated
     */
    @Deprecated
    public static HashMap<Text, IntWritable> readDictionary(Configuration conf, Path dict)
            throws IOException {

        SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(dict));

        HashMap<Text, IntWritable> dictMap = new HashMap<>();
        Text key = new Text();
        IntWritable value = new IntWritable();

        while (reader.next(key, value)) {
            dictMap.put(new Text(key), new IntWritable(value.get()));
        }
        return dictMap;
    }

    /**
     * Reads the tokenized document
     *
     * @param conf
     * @param input
     * @return Returns the document tokens (StringTuples) in a HashMap
     * @throws IOException
     * @deprecated
     */
    @Deprecated
    public HashMap<Text, StringTuple> readTokenizedDocument(Configuration conf, Path input)
            throws IOException {

        SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(input));

        Text key = new Text();
        StringTuple value = new StringTuple();
        HashMap<Text, StringTuple> tokensMap = new HashMap<>();

        while (reader.next(key, value)) {
            tokensMap.put(new Text(key), new StringTuple(value.getEntries()));
        }
        return tokensMap;
    }
}

// End InputData.java
