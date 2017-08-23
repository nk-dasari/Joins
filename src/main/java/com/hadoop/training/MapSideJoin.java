package com.hadoop.training;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;
import org.mortbay.log.Log;


public class MapSideJoin {

	final static Logger logger = Logger.getLogger(MapSideJoin.class);
	
	//File1: STATE,YEAR,MAXTEMP,MINTEMP
	//File2: STATE,GOV
	
	public static class MapJoinMapper extends Mapper<LongWritable,Text,Text,Text>{
		
		Map<String,String> stateMap = new HashMap<String,String>();
		
		protected void setup(Context context) throws IOException,
				InterruptedException {
			
			FileSystem fs = FileSystem.get(context.getConfiguration());
			URI[] uri = context.getCacheFiles();
			logger.info("File is:"+uri[0].getPath().toString());
			FSDataInputStream inStream = fs.open(new Path(uri[0]));
		    DataInputStream in = new DataInputStream(inStream);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String line = new String();
			line = br.readLine();
			while(line !=null){
				String[] words = line.split(",");
				stateMap.put(words[0], words[1]);
				line = br.readLine();
			}
			br.close();
			inStream.close();
			in.close();
		}
	
		@Override
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			
			//File1: STATE,YEAR,MAXTEMP,MINTEMP
			String state = value.toString().split(",")[0];
			if(stateMap.containsKey(state)){
				context.write(new Text(state), value);
			}
			
		}
	}
		
	
		
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "MapSide Join Example");
		job.setJarByClass(MapSideJoin.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
/*		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);*/
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(MapJoinMapper.class);
		FileInputFormat.addInputPath(job, new Path(args[0])); //file1
		FileOutputFormat.setOutputPath(job, new Path(args[1])); 
		job.addCacheFile(new URI(args[2])); //file2
		job.waitForCompletion(true);
	}

}