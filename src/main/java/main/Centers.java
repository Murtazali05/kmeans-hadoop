package main;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class Centers {
    public static final Integer k = 2;

    public static class CenterProperty {
        public static final String name = "center.path";
        public static final String value = "clustering/centers/centers.txt";
    }

    private static List<Point> generateCenters(Configuration configuration) throws IOException {
        List<Point> centers = new ArrayList<>();

        FileWriter writer = new FileWriter(configuration.get(CenterProperty.name));
        writer.write("");

        for (int i = 0; i < k; i++) {
            double x = Math.random() * 20 - 10;
            double y = Math.random() * 20 - 10;
            Point point = new Point(x, y);
            centers.add(point);

            writer.append(String.valueOf(point.getX()))
                    .append(", ")
                    .append(String.valueOf(point.getY()));
            if (i != k-1) writer.append("\n");
        }
        writer.close();
        return centers;
    }

    public static List<Point> readCenters(Configuration conf, Path path) throws IOException {
        List<Point> centers = new ArrayList<>();
        BufferedReader reader = new BufferedReader(new InputStreamReader(FileSystem.get(conf).open(path)));

        while (reader.ready()){
            String fileString = reader.readLine();
            String[] coordinates = fileString.split(",");

            Point point = new Point(Double.parseDouble(coordinates[0]), Double.parseDouble(coordinates[1]));
            centers.add(point);
        }
        reader.close();
        return centers;
    }

    public static void writeCenters(Configuration configuration, List<Point> centers) throws IOException {
        FileWriter writer = new FileWriter(configuration.get(CenterProperty.name));
        writer.write("");

        for (Point point : centers) {
            writer.append(String.valueOf(point.getX()))
                    .append(", ")
                    .append(String.valueOf(point.getY()))
                    .append("\n");
        }
        writer.close();
    }
}
