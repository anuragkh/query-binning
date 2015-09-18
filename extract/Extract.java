package binning;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;

public class Extract extends Configured implements Tool {

	public static class ExtractMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
       
        private char delim;
        private IntWritable one = new IntWritable(1);

        @Override
        public void setup(Context context) {
          delim = (char) Integer.parseInt(context.getConfiguration().get("input.delim"));
        }

        @Override
        public void map(LongWritable key, Text value, Context context)
                    throws IOException, InterruptedException {

          // Get record
          String record = value.toString();

          // Tokenize record to get fields
          StringTokenizer recordTokenizer = new StringTokenizer(record,
                                                                String.valueOf(delim));

          // Assign different key to each field
          int i = 0;
          while(recordTokenizer.hasMoreTokens()) {
            context.write(new Text(recordTokenizer.nextToken() + delim + i), one);
            i++;
          }

        }

 	}

 	public static class ExtractReducer extends Reducer<Text, IntWritable, IntWritable, Text> {

        private char delim;
        private int maxCount;
        private int binSize;

        @Override
        public void setup(Context context) {
          delim = (char) Integer.parseInt(context.getConfiguration().get("input.delim"));
          maxCount = Integer.parseInt(context.getConfiguration().get("max.count"));
          binSize = Integer.parseInt(context.getConfiguration().get("bin.size"));
        }

        @Override
 		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for(IntWritable v: values) {
                count++;
                if(count > maxCount) break;
            }
            int binID = count / binSize;
            if(count < maxCount) {
                context.write(new IntWritable(binID), key);
            }
 		}
 	}

	public int run(String[] args) throws Exception {
        
        int inputDelim = Integer.parseInt(args[2]);
        int maxCount = Integer.parseInt(args[3]);
        int numReducers = Integer.parseInt(args[4]);
        int binSize = maxCount / numReducers;

        System.out.println("max count = " + maxCount);
        System.out.println("num reducers = " + numReducers);
        System.out.println("bin size = " + binSize);

        Configuration conf = getConf();
        conf.setInt("bin.size", binSize);
        conf.setInt("max.count", maxCount);
        conf.setInt("input.delim", inputDelim);
        conf.set("mapred.reduce.tasks.speculative.execution", "false");

        Job job = new Job(conf, "extract");
        job.setNumReduceTasks(numReducers);

        job.setJarByClass(Extract.class);
        job.setMapperClass(ExtractMapper.class);
        job.setReducerClass(ExtractReducer.class);
        job.setInputFormatClass(TextInputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Extract(), args);
        System.exit(res);
    }
}