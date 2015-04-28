/**
 * @author Kalaiselvam ( 113260015)
 * @version 1.0
 * This class will be used to generate the bin-data.txt to be used to generate the repotr
 *
 *
 */
package com.mmu;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

public class ProductHistogram 
{
	public static Logger log = Logger.getLogger(ProductHistogram.class);
	public static Map<String, Integer> recordMap = new HashMap<>();

	/**
	 * This inner class which be mapper class which will be use to split the output from
	 * average-rating.txt to 
	 */
	public static class TokenizerMapperHisto extends	Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text bin = new Text();

		/**
		 * This method use to split the message
		 * 
		 */
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException 
		{
			String token[] = Master.splitMessage(value.toString(),",");
		
			log.info("The token size: "+ token.length);
			
			
			for( String temp: token)
			{
				log.info("The token: "+ temp);
			}
			
			
			if (token.length == 2) 
			{
				bin.set(String.valueOf(returnBinId(token[1])));
				context.write(bin,one);
			}
		}
		
		
		/**
		 * This method return the BIN id based on the count given
		 * 
		 * @param count average count
		 * @return bin id
		 */
		protected int returnBinId( String count )
		{
			double countTemp = Double.parseDouble(count);
			
			if( countTemp >=0 && countTemp < 0.5 )
			{
				return 1;
			}
			
			else if( countTemp >=0.5 && countTemp <1 )
			{
				return 2;
			}
			
			else if( countTemp >=1 && countTemp <1.5 )
			{
				return 3;
			}
			
			else if( countTemp >=1.5 && countTemp <2 )
			{
				return 4;
			}
			
			else if( countTemp >=2 && countTemp <2.5 )
			{
				return 5;
			}
			else if( countTemp >=2.5 && countTemp <3 )
			{
				return 6;
			}
			else if( countTemp >=3 && countTemp <3.5 )
			{
				return 7;
			}
			
			else if( countTemp >=3.5 && countTemp <4 )
			{
				return 8;
			}
			
			else if( countTemp >=4.0 && countTemp <4.5 )
			{
				return 9;
			}		
			
			else
			{
				return 10;				
			}
		}
		
	}
	
	/**
	 * This class will be used as Reduce , reduce the data from the mapper class above to be 
	 * and later to generate bin-data.txt
	 * 
	 *
	 */
	public static class IntSumReducerHisto extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;

			for (IntWritable val : values) 
			{
				sum += val.get();
			}

			result.set(sum);
			context.write(key, result);
			
			recordMap.put(key.toString(), sum);
		}
	}

	/**
	 * This method will be the starting point for this class
	 *  
	 * @param inputPath where is the average file  is located in hfs
	 * @param outputPath where the output filed need to be generated
	 * @throws Exception
	 */
	public void productHistogram(String inputPath, String outputPath)
			throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "ProductHistogram");

		job.setJarByClass(ProductHistogram.class);

		job.setMapperClass(TokenizerMapperHisto.class);
		job.setReducerClass(IntSumReducerHisto.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		job.waitForCompletion(true);
		
		 FileSystem fs = FileSystem.get(conf);
		    
		    
		 String formattedPath = outputPath+Master.fileSequence;
		 /*
		  * will copied the file to local directory based on the parameter in the args from the main method   
		  */
		 fs.copyToLocalFile(new Path(formattedPath), new Path(Master.localCopiedPathBin));
		 
		 log.info("*********THE BIN DATA COMPLETED*******");
	}
}
