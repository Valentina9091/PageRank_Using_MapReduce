import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * @author Valentina Palghadmal
 *
 *        This Program computes the inverted index for Wikipedia documents using
 *        Hadoop MapReduce.
 */
public class InvertedIndex extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(InvertedIndex.class);

	/**
	 * This is the main method used to execute code. It takes two input
	 * arguments <Input folder> <output folder>
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new InvertedIndex(), args);
		System.exit(res);
		LOG.info("End of Inverted Index program");
	}

	/**
	 * This is the driver function to configure and execute map reduce job.
	 */
	public int run(String[] args) throws Exception {
		if (args.length < 2) {
			LOG.info("Missing arguments. There should be two arguments source and destination folder");
		}
		Job job = Job.getInstance(getConf(), "Inverted Index");
		job.setJarByClass(this.getClass());
		FileInputFormat.addInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(ParseMapper.class);
		job.setReducerClass(CalculateReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		return job.waitForCompletion(true) ? 0 : 1;

	}

	/**
	 * This is the static class to read the file line by line and extract id and
	 * text and generate key value pairs where
	 *
	 * @key -> Text(word)
	 * @value -> Text(document id)
	 *
	 */
	public static class ParseMapper extends
			Mapper<LongWritable, Text, Text, Text> {
		final Pattern documentIdPattern = Pattern.compile("<id>(.*?)</id>");
		final Pattern textPattern = Pattern.compile("<text(.*?)>(.+?)</text>");
		Matcher matcher;
		Text id = new Text();

		/**
		 * This method is used the read the inputs & generate key value pairs
		 * key -> Text(word) value -> Text(document id). This code only
		 * considers valid English alphabets and ignores number and special
		 * characters​
		 * 
		 * @param offset
		 *            : LongWritable
		 * @param lineText
		 *            : Text
		 * @param context
		 * @throws IOException
		 * @throws InterruptedException
		 */
		public void map(LongWritable offset, Text inputLine, Context context)
				throws IOException, InterruptedException {
			if (!inputLine.toString().isEmpty()) {
				matcher = documentIdPattern.matcher(inputLine.toString());
				if (matcher.find())
					id = new Text(matcher.group(1));
				matcher = textPattern.matcher(inputLine.toString());
				if (matcher.find()) {
					// considers valid English alphabets and ignores number and
					// special characters
					String input = matcher.group(2).replaceAll("[^a-zA-Z]+",
							" ");
					StringTokenizer token = new StringTokenizer(input, " ");
					while (token.hasMoreTokens()) {
						String word = token.nextToken().trim().toLowerCase();
						context.write(new Text(word), id);
					}
				}
			}
		}
	}

	/**
	 * 
	 * This is a static reducer class takes the input from the mapper and
	 * generates word and text of document id separated by comma(,)
	 */
	public static class CalculateReducer extends
			Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> documentIds,
				Context context) throws IOException, InterruptedException {
			Boolean flag = true;
			StringBuffer buffer = new StringBuffer();
			for (Text id : documentIds) {
				if (!flag)
					buffer.append(",");
				buffer.append(id);
				flag = false;
			}
			context.write(key, new Text(buffer.toString()));
		}
	}
}
