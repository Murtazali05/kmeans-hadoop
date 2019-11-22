package main;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Map
        extends Mapper<Object, Text, Text, Point> {

    private List<Point> centers = new ArrayList<>();

    @Override
    protected void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        centers = Centers.read(conf, new Path(conf.get(Centers.CenterProperty.name)));

        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(new Path(Centers.CenterProperty.name))) {
            fs.delete(new Path(Centers.CenterProperty.name), true);
        }
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // key - номер строки в файле, value - точки. Находим ближайшие центроиды для всех точек.
        String[] coordinates = value.toString().split(",");
        Point point = new Point(Double.parseDouble(coordinates[0]), Double.parseDouble(coordinates[1]));

        double minDist = Double.MAX_VALUE;
        int minIndex = 0, index = 0;

        for (Point center : centers) {
            if (center.dintance(point) < minDist) {
                minDist = center.dintance(point);
                minIndex = index;
            }
            index++;
        }

        context.write(new Text(centers.get(minIndex).toString()), point);
    }
}
