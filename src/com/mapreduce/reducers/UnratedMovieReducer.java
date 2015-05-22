package com.mapreduce.reducers;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UnratedMovieReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text Key, Iterable<Text> Values, Context context)
			throws IOException, InterruptedException {

		System.out.println("UnratedMovieReducer : Started");
		int count = 0;

		for (Text val : Values) {
			String[] row = val.toString().split("-");
			count = Integer.parseInt(row[0]);
			if (count == 0) {

				context.write(new Text(Key.toString().split("\t")[0]), new Text(Key.toString()
						.split("\t")[1] + "\t" + Key.toString().split("\t")[2]));
			}

		}

		System.out.println("UnratedMovieReducer : Completed");
		}

}
