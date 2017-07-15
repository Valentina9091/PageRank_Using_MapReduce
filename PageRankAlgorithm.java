import java.io.IOException;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
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
 *        This Program computes the PageRanks of an input set of hyper-linked
 *        Wikipedia documents using Hadoop MapReduce.
 */
public class PageRankAlgorithm extends Configured implements Tool {
	private static final String N_VALUE = "nValue";
	private static final String DAMPING_FACTOR = "dampingFactor";
	private static final String FILENAME_CONCATENATER = "Job";
	private static final String LINK_SEPARATOR = "@#@";
	private static final String SEPARATOR = "##";

	private static final Logger LOG = Logger.getLogger(PageRankAlgorithm.class);

	/**
	 * This is the main method used to execute code. It takes two input
	 * arguments <Input folder> <output folder>
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			LOG.info("Missing arguments. There should be two arguments source and destination folder");
		} else {
			int res = ToolRunner.run(new PageRankAlgorithm(), args);
			System.exit(res);
			LOG.info("End of Page Rank program");
		}
	}

	/**
	 * This is the driver function to configure and execute map reduce job. It
	 * consist of three job configuration. 1. Job to calculate value to N. 2.
	 * Job to extract the title and outlinks 2. Job to calculate page rank and
	 * iterate it 10 times. 3. Job to sort the pages according to page rank in
	 * descending order.
	 */
	public int run(String[] args) throws Exception {
		FileSystem fileSystem = FileSystem.get(getConf());

		// Job to calculate the value of N
		Job countJob = Job.getInstance(getConf(), "Calculate Value Of N");
		countJob.setJarByClass(this.getClass());
		FileInputFormat.addInputPaths(countJob, args[0]);
		FileOutputFormat.setOutputPath(countJob, new Path(args[1]));
		countJob.setMapperClass(CountDocumentsMapper.class);
		countJob.setReducerClass(CountDocumentReducer.class);
		countJob.setMapOutputValueClass(IntWritable.class);
		countJob.setOutputKeyClass(Text.class);
		countJob.setOutputValueClass(IntWritable.class);
		countJob.waitForCompletion(true);
		fileSystem.delete(new Path(args[1]), true);

		// Calculated value of N
		long nVal = countJob.getCounters()
				.findCounter("totalDocuments", "totalDocuments").getValue();
		LOG.info("Calculated N value = " + nVal);

		// Job to extract the title and outlinks from input documented.
		Job parseJob = Job.getInstance(getConf(), "Parse Documents");
		parseJob.setJarByClass(this.getClass());
		FileInputFormat.addInputPaths(parseJob, args[0]);
		FileOutputFormat.setOutputPath(parseJob, new Path(args[1]
				+ FILENAME_CONCATENATER + "0"));
		// pass the value to job configuration
		parseJob.getConfiguration().setStrings(N_VALUE, nVal + "");
		parseJob.setMapperClass(LinkGraphMapper.class);
		parseJob.setReducerClass(LinkGraphReducer.class);
		parseJob.setOutputKeyClass(Text.class);
		parseJob.setOutputValueClass(Text.class);
		parseJob.waitForCompletion(true);
		LOG.info("End of Parsing documents");

		// Job to calculate the page rank and iterate it for 10 times
		int i = 1;
		int n = 10;
		for (i = 1; i <= n; i++) {
			Job pageRankJob = Job.getInstance(getConf(), "Page Rank");
			pageRankJob.setJarByClass(this.getClass());
			FileInputFormat.addInputPaths(pageRankJob, args[1]
					+ FILENAME_CONCATENATER + (i - 1));
			FileOutputFormat.setOutputPath(pageRankJob, new Path(args[1]
					+ FILENAME_CONCATENATER + i));
			pageRankJob.getConfiguration().set(DAMPING_FACTOR, "0.85F");
			pageRankJob.setMapperClass(MapRank.class);
			pageRankJob.setReducerClass(ReduceRank.class);
			pageRankJob.setOutputKeyClass(Text.class);
			pageRankJob.setOutputValueClass(Text.class);
			pageRankJob.waitForCompletion(true);
			fileSystem.delete(new Path(args[1] + FILENAME_CONCATENATER
					+ (i - 1)), true);
		}

		// Job to sort the pages according to page rank in descending order.
		Job job3 = Job.getInstance(getConf(), "pagerankSort");
		job3.setJarByClass(this.getClass());
		FileInputFormat.addInputPaths(job3, args[1] + FILENAME_CONCATENATER
				+ (i - 1));
		FileOutputFormat.setOutputPath(job3,
				new Path(args[1] + "_sortedOutput"));
		job3.setMapperClass(SortMapper.class);
		job3.setReducerClass(SortReducer.class);
		job3.setMapOutputKeyClass(DoubleWritable.class);
		job3.setMapOutputValueClass(Text.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(DoubleWritable.class);
		job3.waitForCompletion(true);
		fileSystem.delete(new Path(args[1] +FILENAME_CONCATENATER + (i - 1)), true);


		// To ensure that job exits.
		return 1;

	}

	/**
	 * This is the static class to read the file line by line and check if title
	 * tag is present. and generate key value pairs where
	 *
	 * @key -> Text(Title)
	 * @value -> IntWritable(1)
	 *
	 */
	public static class CountDocumentsMapper extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		private static final Pattern titlePattern = Pattern
				.compile("<title>(.*?)</title>");

		/**
		 * This method is used the read the inputs & generate key value pairs
		 * key -> Text(title) value -> IntWritable(1)​
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
			String line = inputLine.toString();
			if (line != null && !line.isEmpty()) {
				Text currentUrl = new Text();
				// Checking title pattern to ignore documents which does not
				// have title.
				Matcher matcher1 = titlePattern.matcher(line);

				if (matcher1.find()) {
					currentUrl = new Text(matcher1.group(1));
					context.write(new Text(currentUrl), new IntWritable(1));
				}
			}
		}
	}

	/**
	 * 
	 * This is a static reducer class takes the input from the mapper and sets
	 * the counter for total number of documents
	 *
	 */
	public static class CountDocumentReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		int counter = 0;

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			this.counter++;
		}

		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			context.getCounter("totalDocuments", "totalDocuments").increment(
					counter);
		}
	}

	/**
	 * This is a static class mapper class to extract the title and text tag
	 * from input file.
	 */
	public static class LinkGraphMapper extends
			Mapper<LongWritable, Text, Text, Text> {

		final Pattern titlePattern = Pattern.compile("<title>(.+?)</title>");
		final Pattern textPattern = Pattern.compile("<text(.*?)>(.+?)</text>");
		final Pattern linksSyntax = Pattern
				.compile("(?<=[\\[]{2}).+?(?=[\\]])");
		Text title = new Text();
		Matcher matcher;
		static int count = 0;

		/**
		 * This method is used the read the inputs & generate key value pairs
		 * key -> Text(title) value -> Text(link to the respective page)​
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
			String link = "";
			if (!inputLine.toString().isEmpty()) {
				//Extract if the title contents if present
				matcher = titlePattern.matcher(inputLine.toString());
				if (matcher.find())
					title = new Text(matcher.group(1));
				//Extract the links from text tag if present
				matcher = textPattern.matcher(inputLine.toString());
				if (matcher.find()) {
					matcher = linksSyntax.matcher(matcher.group());
					link = "";
					while (matcher.find()) {
						link = matcher.group();
						// check if nested links are present if yes then ignore
						// outer link and get the inner link.
						while (link.contains("[[")) {
							int in = link.indexOf("[[");
							link = link.substring(in + 2);
						}
						// if (!link.isEmpty() || link != null) {
						/*
						 * if(!link.trim().equalsIgnoreCase(title.toString().trim
						 * ())) link="";
						 */

						context.write(title, new Text(link));
						// }
					}
				}
				// If title is present but contains no link then write the title
				// to context object with empty link.
				if (link.isEmpty() && !title.toString().isEmpty())
					context.write(title, new Text(link));
			}
		}
	}

	/**
	 * 
	 * This is a static reducer class takes the input from the mapper,
	 * calculates initial page rank value (1/N) and stores output in form of
	 * title ##initial_PageRank##links separated by @#@
	 * 
	 */
	public static class LinkGraphReducer extends
			Reducer<Text, Text, Text, Text> {
		double nValue;

		public void setup(Context context) throws IOException,
				InterruptedException {
			nValue = context.getConfiguration().getDouble(N_VALUE, 1);
		}

		/**
		 * This is overridden method that takes the input from the map class
		 * calculates initial page rank value
		 * 
		 * @param key
		 *            : Text format (title)
		 * @param values
		 *            : Iterable<Text>
		 * @param context
		 * @throws IOException
		 * @throws InterruptedException
		 */
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			StringBuilder sb = new StringBuilder();
			// Calculating initial page rank value
			double initialRank = 1 / nValue;

			sb.append(SEPARATOR + initialRank + SEPARATOR);
			boolean flag = true;

			for (Text value : values) {
				if (!flag)
					sb.append(LINK_SEPARATOR);
				sb.append(value.toString());
				flag = false;
			}
			context.write(key, new Text(sb.toString().trim()));
		}
	}

	/**
	 * 
	 * This is a static map class to which considers input file as output file
	 * of link graph job and generate key value pairs where key -> Text(link)
	 * value -> Text(title pageRank count_outlinks
	 */
	public static class MapRank extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable offset, Text inputLine, Context context)
				throws IOException, InterruptedException {
			String line = inputLine.toString();
			String[] value = line.split(SEPARATOR);
			
			//mark page to consider it as valid page
			context.write(new Text(value[0].trim()), new Text("!"));

			if (value.length == 3) {
				//Storing original links to the page
				context.write(new Text(value[0].trim()), new Text("^" + value[2]));

				String[] links = value[2].split(LINK_SEPARATOR);
				for (int i = 0; i < links.length; i++) {
					context.write(new Text(links[i]), new Text(value[0] + SEPARATOR
							+ value[1] + SEPARATOR + links.length));
				}
			}

		}
	}

	/**
	 * 
	 * This is a static reducer class calculates the page rank and outputs if
	 * the page if its a valid page. It stores output in form of title ##
	 * PageRank ## links separated by @#@
	 *
	 */
	public static class ReduceRank extends Reducer<Text, Text, Text, Text> {
		static String temp = "";
		static String keyvalue = "";
		double dampingFactor;

		public void setup(Context context) throws IOException,
				InterruptedException {
			dampingFactor = context.getConfiguration().getDouble(
					DAMPING_FACTOR, 0.85f);
		}

		@Override
		public void reduce(Text key, Iterable<Text> value, Context context)
				throws IOException, InterruptedException {
			boolean isExistingPage = false;
			String[] splitLinks;
			float sumOfOtherPageRanks = 0f;
			String links = "";
			String pageWithRank;

			for (Text values : value) {
				pageWithRank = values.toString();

				if (pageWithRank.equals("!")) {
					isExistingPage = true;
					continue;
				}

				if (pageWithRank.startsWith("^")) {
					links = SEPARATOR + pageWithRank.substring(1);
					continue;
				}

				splitLinks = pageWithRank.split(SEPARATOR);

				float pageRank = Float.valueOf(splitLinks[1]);
				int countOutLinks = Integer.valueOf(splitLinks[2]);
				sumOfOtherPageRanks += (pageRank / countOutLinks);
			}
			// Check if the page is not a noisy pages(invalid page)
			if (!isExistingPage)
				return;
			double newRank = (dampingFactor * sumOfOtherPageRanks) + (1-dampingFactor);

			context.write(key, new Text(SEPARATOR + newRank + links));
		}
	}

	/**
	 * 
	 * This map class is used to sort the page rank by using the inbuilt feature
	 * of map reduce. It generate key value pairs where key ->
	 * DoubleWritable(rank) value -> Text(title)
	 */
	public static class SortMapper extends
			Mapper<LongWritable, Text, DoubleWritable, Text> {
		public void map(LongWritable offset, Text inputLine, Context context)
				throws IOException, InterruptedException {
			String line = inputLine.toString();
			String[] array = line.split(SEPARATOR);
			double rank = Double.parseDouble(array[1]);
			context.write(new DoubleWritable(-1 * rank), new Text(array[0]));
		}
	}

	/**
	 * 
	 * This class is to stored the sorted values in descending. It generates the
	 * key value pairs where key > Text(title) value -> DoubleWritable(rank)
	 *
	 */
	public static class SortReducer extends
			Reducer<DoubleWritable, Text, Text, DoubleWritable> {

		public void reduce(DoubleWritable rank, Iterable<Text> title,
				Context context) throws IOException, InterruptedException {
			double tempPageRank = 0;
			// multiplying by -1 to sort it to get original value
			tempPageRank = rank.get() * -1;
			for (Text pageTitle : title) {
				context.write(new Text(pageTitle), new DoubleWritable(tempPageRank));
			}
		}
	}
}
