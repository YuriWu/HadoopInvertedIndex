import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class AveCountSorter {

		public static class AveCountSorterMapper extends Mapper<Object, Text, Text, Text>
		{

			@Override
			protected void map(Object key, Text value,
					Mapper<Object, Text, Text, Text>.Context context)
					throws IOException, InterruptedException {
					
				String curLine = value.toString();
				String[] splits = curLine.split("\t");
				String[] splits2 = splits[splits.length - 1].split(",");
				String aveCntStr = splits2[0];
				String content = splits[0] + "\t" + splits2[1];
				context.write(new Text(aveCntStr), new Text(content));	
			}
		}
		
		public static class AveCountSorterReducer extends Reducer<Text, Text, Text, Text>
		{

			@Override
			protected void reduce(Text key, Iterable<Text> values,
					Reducer<Text, Text, Text, Text>.Context context)
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
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			
			System.exit(job.waitForCompletion(true)?0:1);  
		}
}
