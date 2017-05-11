package hadoopDataModel;

/**
 * Created by Boris on 24/04/2017.
 */
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;


public class Point implements WritableComparable<Point>{

    private int clusterID;
    private IntWritable pointID;
    private DoubleWritable[] vector;

    public Point(){super();}

    public Point(DoubleWritable[] vector){
        super();
        this.vector =vector;    }

    public Point(Double[] vector){
        super();
        this.vector = new DoubleWritable[vector.length];
        for(int i=0; i<vector.length; i++){
            this.vector[i] = new DoubleWritable(vector[i]);
        }
    }

    public Point(DoubleWritable[] vector, IntWritable id){
        super();
        this.vector = new DoubleWritable[vector.length];
        for(int i=0; i<vector.length; i++){
            this.vector[i] = (vector[i]);
        }
        this.pointID = id;
    }

    public Point(Double[] vector, int pointID){
        super();
        this.vector = new DoubleWritable[vector.length];
        for(int i=0; i<vector.length; i++){
            this.vector[i] = new DoubleWritable(vector[i]);
        }
        this.pointID = new IntWritable(pointID);
    }
    public Point(Double[] vector, int pointID, int clusterID){
        super();
        this.vector = new DoubleWritable[vector.length];
        for(int i=0; i<vector.length; i++){
            this.vector[i] = new DoubleWritable(vector[i]);
        }
        this.pointID = new IntWritable(pointID);
        this.clusterID = clusterID;
    }

    public static Point read(DataInput in)throws IOException{
        Point p = new Point();
        p.readFields(in);
        return p;
    }
    @Override
    public void write(DataOutput out)throws IOException{
        out.writeInt(clusterID);
        pointID.write(out);
        writeVector(vector, out);
    }
    @Override
    public void readFields(DataInput in)throws IOException{
        clusterID = in.readInt();
        pointID = new IntWritable(in.readInt());
        int length = in.readInt();
        set(length, in);
    }

    @Override
    public int compareTo(Point otherPoint){
        return compareVector(vector, otherPoint.getVector());
    }

    @Override //se qualcosa non va migliora questo
    public int hashCode() {
        final int prime = 31;
        int result = pointID.hashCode();
        result = prime * result + vector.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Point other = (Point) obj;
        if (vector == null) {
            if (other.getVector() != null)
                return false;
        } else if (this.hashCode()!=obj.hashCode())
            return false;
        return true;
    }

    private int compareVector(DoubleWritable[] vector, DoubleWritable[] otherVector){
        Double[] tmp = new Double[vector.length];
        double sum =0;
        for(int i =0; i<vector.length; i++){
            tmp[i] = vector[i].get() - otherVector[i].get();
            sum += tmp[i];
        }
        return (int)sum;
    }

    private void set(int length, DataInput in)throws IOException{
        DoubleWritable[] vect = new DoubleWritable[length];
        for(int i=0; i<length; i++){
            vect[i] = new DoubleWritable(in.readDouble());
        }
        this.vector = vect;
    }

    private void writeVector(DoubleWritable[] vector, DataOutput out)throws IOException{
        out.writeInt(vector.length);
        for(DoubleWritable comp :vector){
            comp.write(out);
        }
    }

    public IntWritable getPointID(){
        return this.pointID;
    }

    public void setPointID(int id) {
        this.pointID = new IntWritable(id);
    }


    public void setClusterID(int id) {
        this.clusterID = id;
    }
    public int getClusterID() {
        return clusterID;
    }

    public DoubleWritable[] getVector(){
        return vector;
    }

}
