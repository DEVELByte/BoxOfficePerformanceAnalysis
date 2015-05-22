package com.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

import com.mapreduce.chains.HighestRatingChain;
import com.mapreduce.chains.OldestMovieChain;
import com.mapreduce.chains.RecordUpdateChain;
import com.mapreduce.chains.UnratedMovieChain;

public class MainDriver {

	public static void main(String[] args) throws Exception {

		ToolRunner.run(new Configuration(), new OldestMovieChain(), args);
		ToolRunner.run(new Configuration(), new RecordUpdateChain(), args);
		ToolRunner.run(new Configuration(), new UnratedMovieChain(), args);
		ToolRunner.run(new Configuration(), new HighestRatingChain(), args);
	}
}