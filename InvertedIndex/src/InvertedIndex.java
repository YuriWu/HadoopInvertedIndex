import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InvertedIndex {

	
	public static class InvertedIndexMapper extends Mapper<Object, Text, Text, Text> {
		
		private Text outKey = new Text();
		private Text outVal = new Text();
		
		@Override
		protected void map(Object key, Text value,
				Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			StringTokenizer tokens = new StringTokenizer(value.toString());
			FileSplit split = (FileSplit) context.getInputSplit();
			
			while(tokens.hasMoreTokens())
			{
				String token = tokens.nextToken();
				outKey.set(token + ":" + split.getPath());
				outVal.set("1");
				context.write(outKey, outVal);
			}
		}
	}
	
	public static class InvertedIndexCombiner extends Reducer<Text, Text, Text, Text>
	{

		private Text outKey = new Text();
		private Text outVal = new Text();
		
		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			String[] keys = key.toString().split(":");
			int sum = 0;
			for(Text val : values) {
				sum += Integer.parseInt(val.toString());
			}
			outKey.set(keys[0]);
			int index = keys[keys.length - 1].lastIndexOf('/');
			outVal.set(keys[keys.length - 1].substring(index +1) + ":" + sum);
			context.write(outKey, outVal);
		}
	}
	
	public static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text>
	{

		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			StringBuffer strBuf = new StringBuffer();
			for(Text text : values) {
				strBuf.append(text.toString() + " ,");
			}
			context.write(key, new Text(strBuf.toString()));
		}
	}
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException
	{
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Inverted Index");
		
		job.setJarByClass(InvertedIndex.class);
		job.setMapperClass(InvertedIndex.InvertedIndexMapper.class);
		job.setCombinerClass(InvertedIndex.InvertedIndexCombiner.class);
		job.setReducerClass(InvertedIndex.InvertedIndexReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true)?0:1);  
	}
}
