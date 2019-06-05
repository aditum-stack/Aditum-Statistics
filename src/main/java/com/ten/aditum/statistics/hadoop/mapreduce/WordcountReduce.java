package com.ten.aditum.statistics.hadoop.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordcountReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text key, Iterable<IntWritable> values, Reducer.Context context) throws IOException, InterruptedException {
        int count = 0;
        for (IntWritable value : values) {
            count++;
        }
        context.write(key, new IntWritable(count));
    }
}
