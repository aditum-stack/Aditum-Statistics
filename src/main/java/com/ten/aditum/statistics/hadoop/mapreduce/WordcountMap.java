package com.ten.aditum.statistics.hadoop.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordcountMap extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();//读取一行数据
        String str[] = line.split("");//因为英文字母是以“ ”为间隔的，因此使用“ ”分隔符将一行数据切成多个单词并存在数组中
        for (String s : str) {//循环迭代字符串，将一个单词变成<key,value>形式，及<"hello",1>
            context.write(new Text(s), new IntWritable(1));
        }
    }
}
