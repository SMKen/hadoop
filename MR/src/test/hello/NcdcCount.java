package test.hello;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.StringTokenizer;

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
public class NcdcCount
{

	public static class NcdcMapper extends Mapper<Object, Text, Text, IntWritable>
	{

//		private final static IntWritable one = new IntWritable(1);
//		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			System.out.print(key+"_");
			System.out.print(value+"_");
			System.out.println(context);
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
			/*StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens())
			{
				word.set(itr.nextToken());
				context.write(word, one);
			}*/
		}
	}

	public static class NcdcReduce extends Reducer<Text, IntWritable, Text, IntWritable>
	{
//		private IntWritable result = new IntWritable();

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
		args = new String[] { "/usr/local/hadoop-2.5.0/etc/hadoop", "/home/big/temp" };
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2)
		{
			System.err.println("Usage: wordcount <in> [<in>...] <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "NCDC COUNT");
		job.setJarByClass(NcdcCount.class);
		job.setMapperClass(NcdcMapper.class);
		job.setCombinerClass(NcdcReduce.class);
		job.setReducerClass(NcdcReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		Path inPath = new Path("/home/big/ncdc/less");
		Path outPath = new Path("/home/big/ncdc/output");
		
//		for (int i = 0; i < otherArgs.length - 1; ++i)
//		{
//			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
//		}

		FileInputFormat.addInputPath(job, inPath);
		
		FileOutputFormat.setOutputPath(job, outPath);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
