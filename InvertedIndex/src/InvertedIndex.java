import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class InvertedIndex {
	
	public static class InvertedIndexMapper extends Mapper<Object, Text, Text, IntWritable> {

		@Override
		protected void map(Object key, Text value,
				Mapper<Object, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			
			StringTokenizer tokens = new StringTokenizer(value.toString());
			FileSplit split = (FileSplit) context.getInputSplit();
			
			while(tokens.hasMoreTokens())
			{
				String token = tokens.nextToken();
				Text outKey = new Text();
				outKey.set(token + ":" + split.getPath());
				context.write(outKey, new IntWritable(1));
			}		
		}
		
	}
	
	public static class InvertedIndexCombiner extends Reducer<Text, IntWritable, Text, IntWritable>
	{

		private Text outKey = new Text();
		private IntWritable outVal = new IntWritable();
		
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			
			String[] keys = key.toString().split(":");
			int sum = 0;
			for(IntWritable val : values) {
				sum += val.get();
			}
			int index = keys[keys.length - 1].lastIndexOf('/');
			outKey.set(keys[0] + ":" + keys[keys.length - 1].substring(index +1));
			outVal.set(sum);
			context.write(outKey, outVal);
		}
	}
	
	public static class InvertedIndexPartitioner extends HashPartitioner<Text, IntWritable>
	{

		@Override
		public int getPartition(Text key, IntWritable value, int numReduceTasks) {
			String term = new String();
			term = key.toString().split(":")[0];
			return super.getPartition(new Text(term), value, numReduceTasks);
		}
		
	}
	
	public static class InvertedIndexReducer extends Reducer<Text, IntWritable, Text, Text>
	{
		static int fileNum = 0;
		static int totalCount = 0;
		static Text curItem = new Text("");
		static List<String> postingList = new ArrayList<String>();
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			int sum;
			sum = 0;
			
			String curFile = key.toString().split(":")[1];
			key = new Text(key.toString().split(":")[0]);
			
			Iterator<IntWritable> valueIter = values.iterator();
			while(valueIter.hasNext())
			{
				int curValue = valueIter.next().get();
				sum += curValue;
			}
			
			if(!curItem.equals(key) && !curItem.toString().equals(""))
			{
				float rate = ( (float)totalCount / (float)fileNum );
				StringBuffer strBuf = new StringBuffer("\t" + rate + ",");
				Iterator<String> listIter = postingList.iterator(); 
				while(listIter.hasNext())
				{
					strBuf.append(listIter.next());
					if(listIter.hasNext())
					{
						strBuf.append(";");
					}
				}
				context.write(curItem, new Text(strBuf.toString()));
				postingList = new ArrayList<String>();
				fileNum = 0;
				totalCount = 0;
			}
			curItem = new Text(key);
			postingList.add(curFile + ":" + sum);
			fileNum += 1;
			totalCount += sum;
		}
		
		@Override
		protected void cleanup(
				Reducer<Text, IntWritable, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			float rate = ( (float)totalCount / (float)fileNum );
			StringBuffer strBuf = new StringBuffer("\t" + rate + ",");
			Iterator<String> listIter = postingList.iterator(); 
			while(listIter.hasNext())
			{
				strBuf.append(listIter.next());
				if(listIter.hasNext())
				{
					strBuf.append(";");
				}
			}
			context.write(curItem, new Text(strBuf.toString()));
			postingList = new ArrayList<String>();
		}
	}
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException
	{
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Inverted Index");
		
		job.setJarByClass(InvertedIndex.class);
		job.setMapperClass(InvertedIndex.InvertedIndexMapper.class);
		job.setCombinerClass(InvertedIndex.InvertedIndexCombiner.class);
		job.setPartitionerClass(InvertedIndex.InvertedIndexPartitioner.class);
		job.setReducerClass(InvertedIndex.InvertedIndexReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true)?0:1);  
	}
}
