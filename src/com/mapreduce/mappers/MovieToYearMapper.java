package com.mapreduce.mappers;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MovieToYearMapper extends Mapper<Object, Text, Text, Text> {

	public void map(Object Key, Text Value, Context context)
			throws IOException, InterruptedException {
		System.out.println("MovieToyearMapper : Started");

		String[] row = Value.toString().split("\\|");

		Text outKey = new Text("movie");
		Text outValue = new Text(row[1] + "-" + row[2]);
		System.out.println("MovieToyearMapper : Completed -->" + outKey.toString() + "--"
				+ outValue.toString());
		context.write(outKey, outValue);

	}

}
