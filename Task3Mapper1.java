package mapreduce.assignment3;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Task3Mapper1 extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	protected void map(LongWritable key,Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		String[] array = value.toString().split("[|]");
		Text company = new Text(array[0]);
		Text product = new Text(array[1]);
		IntWritable num_items = new IntWritable(1);
		if(!("NA".equalsIgnoreCase(company.toString())) && !("NA".equalsIgnoreCase(product.toString())))
			context.write(company, num_items);
	}

	
}
