package mr.extend.singlefile;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class TestMyInputFormat
{

	public static class MapperClass extends Mapper<LongWritable, Text, Text, Text>
	{

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			System.out.println("key:\t " + key);
			System.out.println("value:\t " + value);
			System.out.println("-------------------------");
		}
	}

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException
	{
		Configuration conf = new Configuration();
		Path inPath = new Path("/home/big/ncdc/less");
		Path outPath = new Path("/home/big/ncdc/output");
		FileSystem.get(conf).delete(outPath, true);
		Job job = new Job(conf, "TestMyInputFormat");
		job.setInputFormatClass(SingleFileInputFormat.class);
		job.setJarByClass(TestMyInputFormat.class);
		job.setMapperClass(TestMyInputFormat.MapperClass.class);
		job.setNumReduceTasks(0);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, inPath);
		org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath(job, outPath);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}