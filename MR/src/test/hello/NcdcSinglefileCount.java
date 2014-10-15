package test.hello;

import java.io.IOException;
import java.math.BigDecimal;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * @author Ken 
 * @since Oct 13, 2014
 */
public class NcdcSinglefileCount
{

	public static class NcdcMapper extends Mapper<Object, Text, Text, IntWritable>
	{ 

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{ 
			if(value != null)
			{
				String s = value.toString();
				if(s.length()>31)
				{
					String temp = s.substring(26, 31);
					String year = s.substring(14, 23); 
					try{
						java.math.BigDecimal tmp = new BigDecimal(temp.trim()).setScale(1);
						System.out.println("mapper -> " +year + tmp.floatValue());
						context.write(new Text(year), new IntWritable(tmp.multiply(new BigDecimal(10)).intValue()));
						
					}catch(Exception e){
						
					}
				}
			} 
		}
	}

	public static class NcdcReduce extends Reducer<Text, IntWritable, Text, IntWritable>
	{ 

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
		{
			System.out.println("in reducer");
			int max = Integer.MIN_VALUE;
			for (IntWritable val : values)
			{
				System.out.println(key+"_"+values);
				max = Math.max(max, val.get());
			} 
			context.write(key, new IntWritable(max));
		}
	}

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();  
		Job job = new Job(conf, "NCDC COUNT");
		job.setJarByClass(NcdcSinglefileCount.class);
		job.setMapperClass(NcdcMapper.class);
		job.setCombinerClass(NcdcReduce.class);
		job.setReducerClass(NcdcReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		Path inPath = new Path("/home/big/ncdc/less");
		Path outPath = new Path("/home/big/ncdc/output");
		FileInputFormat.addInputPath(job, inPath);
		
		FileOutputFormat.setOutputPath(job, outPath);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
