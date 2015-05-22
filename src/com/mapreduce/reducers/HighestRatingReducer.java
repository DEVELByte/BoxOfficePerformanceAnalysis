package com.mapreduce.reducers;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.mapreduce.others.ValueComparator;

public class HighestRatingReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text Key, Iterable<Text> Values, Context context)
			throws IOException, InterruptedException {

		System.out.println("HighestRatingReducer : Started  -->"
				+ Key.toString());
		int count = 0;
		HashMap<String, Integer> UnsortedMovieDetails = new HashMap<String, Integer>();
		ValueComparator compair = new ValueComparator(UnsortedMovieDetails);
		TreeMap<String, Integer> sortedMovieDetails = new TreeMap<String, Integer>(
				compair);

		for (Text val : Values) {
			System.out.println("Value -->" + val.toString());
			String[] row = val.toString().split("-");
			UnsortedMovieDetails.put(row[0] + "\t" + row[1] + "\t" + row[2],
					Integer.parseInt(row[4]));
		}

		sortedMovieDetails.putAll(UnsortedMovieDetails);

		for (Map.Entry<String, Integer> x : sortedMovieDetails.entrySet()) {
			if (count <= 2) {
				String[] row = x.getKey().split("\t");
				context.write(new Text(row[0]), new Text(row[1] + "\t" + row[2]
						+ "\t" + x.getValue()));
				count++;
			} else
				break;

		}

		System.out.println("HighestRatingReducer : Completed");
	}

}
