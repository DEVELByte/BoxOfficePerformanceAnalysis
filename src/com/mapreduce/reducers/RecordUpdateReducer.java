package com.mapreduce.reducers;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RecordUpdateReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text Key, Iterable<Text> Values, Context context)
			throws IOException, InterruptedException {

		System.out.println("RecordUpdateReducer : Started");
		int totRatings = 0;
		int count = 0;
		int avgRating = 0;

		for (Text val : Values) {
			String[] row = val.toString().split("-");

			count = Integer.parseInt(row[0]);
			if (count > 0) {
				avgRating = totRatings / count;
			}

		}

		Text outValue = new Text(count + "\t" + avgRating);
		System.out.println("RecordUpdateReducer : Completed -->>"
				+ outValue.toString());
		context.write(Key, outValue);
	}

}
