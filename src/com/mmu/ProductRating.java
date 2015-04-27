package com.mmu;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

public class ProductRating 
{
	 Logger log = Logger.getLogger(ProductRating.class);		
	
  public static class TokenizerMapper extends Mapper<Object, Text, Text, DoubleWritable> 
  {

    private Text product = new Text();

    public void map(Object key, Text value, Context context ) throws IOException, InterruptedException 
    {
      String [] token = Master.splitMessage(value.toString(), "::");	
    
      if(token.length == 4)
      {
    	  product.set(token[1]);
    	  context.write(product, new DoubleWritable( Double.parseDouble(token[2])));
      }
     }
  }

  public static class IntSumReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable> 
  {
    private DoubleWritable result = new DoubleWritable();

    public void reduce(Text key, Iterable<DoubleWritable> values, Context context ) throws IOException, InterruptedException 
    {
      int sum = 0;
      int count =0;
      
      for (DoubleWritable val : values)
      {
        sum += val.get();
        count++;
      }


      result.set((sum/count));
      String formattedMessage = Master.formatMessage(",",key.toString(),result.toString());
      key.set(formattedMessage);
      context.write(key,(DoubleWritable)null);
    }
    
    
    
    
  }

public void productRatingAvarage(String inputPath, String outputPath) throws Exception
  {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "productRating");
    
    job.setJarByClass(ProductRating.class);
    
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(IntSumReducer.class);
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);
    
    FileInputFormat.addInputPath(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));
    
    
    job.waitForCompletion(true);
    
    FileSystem fs = FileSystem.get(conf);
    
    
    String formattedPath = outputPath+Master.fileSequence;
    
    fs.copyToLocalFile(new Path(formattedPath), new Path(Master.localCopiedPathAverage));
    
    log.info("*********THE PRODUCT RATING COMPLETED*******");
    
  }
}