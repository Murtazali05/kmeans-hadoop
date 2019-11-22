package main;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Reduce
        extends Reducer<Text, Point, NullWritable, Text> {

    private List<Point> centers = new ArrayList<>();

    @Override
    protected void reduce(Text key, Iterable<Point> values, Context context) throws IOException, InterruptedException {
        // Входные данные - ценроиды со список точек ближайших к ним. Задача редьюсера вычислить новые центроиды для каждого списка точек.
        double x = Double.parseDouble(key.toString().split(",")[0]);
        double y = Double.parseDouble(key.toString().split(",")[1]);
        int count = 0;

        for (Point value : values) {
            x += value.getX();
            y += value.getY();
            count += 1;
        }

        Point newCenter = new Point(x / (double) count, y / (double) count);
        centers.add(newCenter);

        context.write(null, new Text(newCenter.toString()));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        Configuration conf = context.getConfiguration();
        Centers.writeCenters(conf, centers);
    }
}
