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
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;

public class Segregate extends Configured implements Tool {

    public static class SegregateMapper extends Mapper<Text, Text, IntWritable, Text> {
        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            context.write(new IntWritable(Integer.parseInt(key.toString())), value);
        }
    }

    public static class SegregateReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        
        private char delim;

        @Override
        public void setup(Context context) {
          delim = (char) Integer.parseInt(context.getConfiguration().get("input.delim"));
        }

        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            long count = 0;
            for(Text value: values) {
                if(count > 100000) {
                    break;
                }
                String field = value.toString();
                String fieldValue = field.substring(0, field.indexOf(delim));
                int columnID = Integer.parseInt(field.substring(field.indexOf(delim) + 1));
                context.write(key, new Text(columnID + "\t" + fieldValue));
                count++;
            }
        }
    }

	public int run(String[] args) throws Exception {
        
        int inputDelim = Integer.parseInt(args[2]);
        int numReducers = Integer.parseInt(args[3]);

        System.out.println("input delim = " + inputDelim);
        System.out.println("num reducers = " + numReducers);

        Configuration conf = getConf();
        conf.setInt("input.delim", inputDelim);
        conf.set("mapred.reduce.tasks.speculative.execution", "false");

        Job job = new Job(conf, "segregate");
        job.setNumReduceTasks(numReducers);

        job.setJarByClass(Segregate.class);
        job.setMapperClass(SegregateMapper.class);
        job.setReducerClass(SegregateReducer.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Segregate(), args);
        System.exit(res);
    }
}