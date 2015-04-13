import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class AveCountSorter {

		public static class AveCountSorterMapper extends Mapper<Object, Text, FloatType, Text>
		{

			@Override
			protected void map(Object key, Text value,
					Mapper<Object, Text, FloatType, Text>.Context context)
					throws IOException, InterruptedException {
					
				String curLine = value.toString();
				String[] splits = curLine.split("\t");
				String[] splits2 = splits[splits.length - 1].split(",");
				FloatType aveCntStr = new FloatType(splits2[0]);
				String content = splits[0] + "\t" + splits2[1];
				context.write(aveCntStr, new Text(content));	
			}
		}
		
		public static class FloatTypeComparator extends WritableComparator
		{
			protected FloatTypeComparator()
			{
				super(FloatType.class, true);
			}
			
			@SuppressWarnings("rawtypes")
			@Override
	      public int compare(WritableComparable w1, WritableComparable w2)
	        {
	            FloatType ip1 = (FloatType) w1;
	            FloatType ip2 = (FloatType) w2;
	            return ip1.compareTo(ip2);
	        }
		}
		
		public static class AveCountSorterReducer extends Reducer<FloatType, Text, Text, Text>
		{

			@Override
			protected void reduce(FloatType key, Iterable<Text> values,
					Reducer<FloatType, Text, Text, Text>.Context context)
					throws IOException, InterruptedException {
				
				String valueStr;
				String splits[];
				String keyStr = key.toString();
				for(Text value : values)
				{
					valueStr = value.toString();
					splits = valueStr.split("\t");
					context.write(new Text(splits[0]), new Text(keyStr + "," + splits[1]));
				}
			}
		}
		
		public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
		{
			Configuration conf = new Configuration();
			Job job = new Job(conf, "Average Count Sorter");
			
			job.setJarByClass(AveCountSorter.class);
			job.setMapperClass(AveCountSorter.AveCountSorterMapper.class);
			job.setReducerClass(AveCountSorter.AveCountSorterReducer.class);
			job.setGroupingComparatorClass(FloatTypeComparator.class);
			job.setMapOutputKeyClass(FloatType.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			
			System.exit(job.waitForCompletion(true)?0:1);  
		}
}

