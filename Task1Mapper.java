package com.acadgild.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Task1Mapper extends Mapper<LongWritable,Text,LongWritable ,Text>{
	
	public void map(LongWritable key,Text value,Context context)throws InterruptedException,IOException{
		String[] lineArray=value.toString().split("\\|");
		if(!(lineArray[0].equals("NA") ||lineArray[1].equals("NA")) ){		
			context.write(key, value);
		}
	}
	
}
