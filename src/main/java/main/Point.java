package main;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class Point implements WritableComparable {
    private Double coordinateX, coordinateY;

    public Point() {
    }

    public Point(Double coordinateX, Double coordinateY) {
        this.coordinateX = coordinateX;
        this.coordinateY = coordinateY;
    }

    public Double getX() {
        return coordinateX;
    }

    public Double getY() {
        return coordinateY;
    }

    public Double dintance(Point point) {
        return Math.sqrt(Math.abs(this.coordinateX - point.coordinateX) + Math.abs(this.coordinateY - point.coordinateY));
    }

    public int compareTo(Object o) {
        Point other = (Point) o;

        if (!this.equals(other)) {
            return 1;
        }
        return 0;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(coordinateX);
        dataOutput.writeDouble(coordinateY);
    }

    public void readFields(DataInput dataInput) throws IOException {
        coordinateX = dataInput.readDouble();
        coordinateY = dataInput.readDouble();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Point point = (Point) o;
        return coordinateX.equals(point.coordinateX) &&
                coordinateY.equals(point.coordinateY);
    }

    @Override
    public int hashCode() {
        return Objects.hash(coordinateX, coordinateY);
    }

    @Override
    public String toString() {
        return coordinateX + ", " + coordinateY;
    }
}
