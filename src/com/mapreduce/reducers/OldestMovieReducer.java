package com.mapreduce.reducers;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class OldestMovieReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text Key, Iterable<Text> Values, Context context)
			throws IOException, InterruptedException {
		System.out.println("OldestMovieReducer : Started");
		int MaxYear = 9999;
		String Movie = null;

		for (Text val : Values) {
			String[] row = val.toString().split("-");
			System.out.println(row[0] + "--->" + row[1]);
			if (MaxYear > Integer.parseInt(row[1])
					&& Integer.parseInt(row[1]) != 0) {

				MaxYear = Integer.parseInt(row[1]);
				Movie = row[0];
				System.out.println(MaxYear + "--->" + Movie);
			}
		}

		Text outKey = new Text();
		Text outvalue = new Text();
		if (MaxYear == 9999) {
			outKey.set("Oldest Movie:");
			outvalue.set("None Of the Movies have Year Data:");
		} else {
			outKey.set(Movie);
			outvalue.set("" + MaxYear);
		}
		System.out.println("OldestMovieReducer : Completed");
		context.write(outKey, outvalue);
	}

}
