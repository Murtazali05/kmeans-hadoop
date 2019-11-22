package main;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.util.List;

public class Main {

    public static void main(String[] args) throws Exception {

        Path input = new Path(args[0]);
        Path output = new Path(args[1]);

        Configuration conf = new Configuration();
        conf.set(Centers.CenterProperty.name, Centers.CenterProperty.value);

        List<Point> previous = Centers.read(conf, new Path(conf.get(Centers.CenterProperty.name)));
        List<Point> current;

        boolean changed = false;
        int code = 0;

        Job job;

        while (!changed) {
            job = Job.getInstance(conf, "kMeans");
            job.setJarByClass(Main.class);

            job.setInputFormatClass(TextInputFormat.class);
            FileInputFormat.addInputPath(job, input);

            job.setMapperClass(Map.class);
            job.setReducerClass(Reduce.class);

            job.setOutputFormatClass(TextOutputFormat.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Point.class);
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Text.class);
            FileOutputFormat.setOutputPath(job, output);

            FileSystem fs = FileSystem.get(conf);
            if (fs.exists(output)) {
                fs.delete(output, true);
            }
            code = job.waitForCompletion(true) ? 0 : 1;

            current = Centers.read(conf, new Path(conf.get(Centers.CenterProperty.name)));

            if (current.equals(previous)) {
                changed = true;
            }

            previous = current;
        }

        System.exit(code);
    }
}
