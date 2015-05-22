package com.mapreduce.chains;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import com.mapreduce.MainDriver;
import com.mapreduce.mappers.MovieToRatingsMapper;
import com.mapreduce.reducers.RecordUpdateReducer;

public class RecordUpdateChain extends Configured implements Tool {

	@Override
	public int run(String[] arg0) throws Exception {

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(arg0[2] + "/C_UpdateMovieRecord"), true);
		// Distribute the movie detail file to mappers
		try {
			DistributedCache.addCacheFile(new URI(arg0[1]), conf);
		} catch (Exception e) {
			System.out.println(e.getStackTrace());

		}

		Job job = new Job(conf, "C_UpateMovieRecord");
		job.setJarByClass(MainDriver.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		//job.setInputFormatClass(TextInputFormat.class);
		
		job.setMapperClass(MovieToRatingsMapper.class);
		job.setReducerClass(RecordUpdateReducer.class);

		FileInputFormat.setInputPaths(job, new Path(arg0[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg0[2]
				+ "/C_UpdateMovieRecord"));

		return job.waitForCompletion(true) ? 0 : 1;
	}
}
