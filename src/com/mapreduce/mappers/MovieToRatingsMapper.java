package com.mapreduce.mappers;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MovieToRatingsMapper extends Mapper<Object, Text, Text, Text> {

	private Map<String, String> movieMap = new HashMap<String, String>();

	// private Text outputKey = new Text();
	// private Text outputValue = new Text();

	protected void setup(Context context) throws java.io.IOException,
			InterruptedException {
		System.out.println("MovieToRatingsMapper : Setup -->  Started");
		Path[] files = DistributedCache.getLocalCacheFiles(context
				.getConfiguration());

		for (Path p : files) {
			if (p.getName().equals("movierating.txt")) {
				BufferedReader reader = new BufferedReader(new FileReader(
						p.toString()));
				String line = reader.readLine();
				while (line != null) {
					String[] tokens = line.split("\\|");
					String id = tokens[1];
					String rating = tokens[2];
					int count = 0;
					int totRating = 0;

					if (movieMap.get(id) == null) {
						movieMap.put(id, 1 + "-" + rating);
						totRating = Integer.parseInt(rating);
					} else {
						count = Integer
								.parseInt(movieMap.get(id).split("-")[0]) + 1;
						totRating = Integer.parseInt(movieMap.get(id).split(
								"-")[1])
								+ Integer.parseInt(rating);
						movieMap.remove(id);
						movieMap.put(id, count + "-" + totRating);
					}
					line = reader.readLine();
				}
			}
			System.out.println("MovieToRatingsMapper : Setup -->  Completed");
		}
		if (movieMap.isEmpty()) {
			throw new IOException("Unable to load Abbrevation data.");
		}
	}

	public void map(Object Key, Text Value, Context context)
			throws IOException, InterruptedException {

		System.out.println("MovieToRatingsMapper : Started");

		String[] row = Value.toString().split("\\|");
		String id = row[0];
		String movieDetail = row[1] + "\t" + row[2];

		if (movieMap.get(id) != null) {
			System.out.println("MovieToRatingsMapper : Rated -->>" + id
					+ "--->" + movieMap.get(id));
			context.write(new Text(id + "\t" + movieDetail),
					new Text(movieMap.get(id)));
		} else {
			System.out.println("MovieToRatingsMapper : Unrated -->>" + id
					+ "--->" + movieMap.get(id));
			context.write(new Text(id + "\t" + movieDetail), new Text(
					0 + "-" + 0));
		}

		System.out.println("MovieToRatingsMapper : Completed");
		// context.write(new Text(id + "\t" + movieDetail), new Text(row[2]));

		// context.write(new Text("Key"), new Text("Value"));
	}
}
