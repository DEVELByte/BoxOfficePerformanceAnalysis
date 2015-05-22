package com.mapreduce.chains;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import com.mapreduce.MainDriver;
import com.mapreduce.mappers.MovieToYearMapper;
import com.mapreduce.reducers.OldestMovieReducer;

public class OldestMovieChain extends Configured implements Tool {

	@Override
	public int run(String[] arg0) throws Exception {
		//
		// Job 1
		//
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(arg0[2] + "/A_OldestMovie"), true);

		Job job = new Job(conf, "A_oldestMovie");
		job.setJarByClass(MainDriver.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(MovieToYearMapper.class);
		job.setReducerClass(OldestMovieReducer.class);

		FileInputFormat.setInputPaths(job, new Path(arg0[0]));
		FileOutputFormat.setOutputPath(job,
				new Path(arg0[2] + "/A_OldestMovie"));
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

}
