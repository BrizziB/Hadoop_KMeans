package hadoopDataModel;

import Utils.ArrayOP;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Boris on 24/04/2017.
 */
public class Centroid implements WritableComparable<Centroid> {


    private int clusterID;
    private IntWritable centroidID;
    private DoubleWritable[] vector;


    public int getClusterID() {
        return clusterID;
    }
    public void setClusterID(int clusterID) {
        this.clusterID = clusterID;
    }

    public Centroid(){super();}

    public Centroid (DoubleWritable[] vect){
        super();
        this.vector = vect;
    }

    public Centroid(Double[] vector){
        super();
        this.vector = new DoubleWritable[vector.length];
        for(int i=0; i<vector.length; i++){
            this.vector[i] = new DoubleWritable(vector[i]);
        }
    }
    public Centroid(DoubleWritable[] vector, IntWritable centroidID){
        super();
        this.vector = new DoubleWritable[vector.length];
        for(int i=0; i<vector.length; i++){
            this.vector[i] = vector[i];
        }
        this.centroidID = centroidID;
    }
    public Centroid(DoubleWritable[] vector, IntWritable centroidID, int id){
        super();
        this.vector = new DoubleWritable[vector.length];
        for(int i=0; i<vector.length; i++){
            this.vector[i] = vector[i];
        }
        this.centroidID = centroidID;
        this.clusterID = id;
    }

    public static Centroid read(DataInput in)throws IOException {
        Centroid p = new Centroid();
        p.readFields(in);
        return p;
    }
    @Override
    public void write(DataOutput out)throws IOException{
        out.writeInt(clusterID);
        centroidID.write(out);
        writeVector(vector, out);
    }
    @Override
    public void readFields(DataInput in)throws IOException{
        clusterID = in.readInt();
        centroidID = new IntWritable(in.readInt());
        int length = in.readInt();
        set(length, in);
    }

    @Override
    public int compareTo(Centroid otherPoint){
        return compareVector(vector, otherPoint.getVector());
    }

    @Override //se qualcosa non va migliora questo
    public int hashCode() {
        final int prime = 31;
        int result = centroidID.hashCode();
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
        Centroid other = (Centroid) obj;
        if (vector == null) {
            if (other.getVector()!= null)
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

    public boolean hasConverged(Centroid newCentroid){

        if(Math.abs((ArrayOP.ArrayMath.subtractArrays(this.getVector(), newCentroid.getVector())).get())<0.001)
            return true;
        return false;

    }


    public IntWritable getCentroidID(){
        return this.centroidID;
    }

    public void setCentroidID(int id) {
        this.centroidID = new IntWritable(id);
    }

    public DoubleWritable[] getVector(){
        return vector;
    }

    public void setVector (DoubleWritable[] vect){
        this.vector = vect;
    }

    public void setVector(Double[] vect){
        DoubleWritable[] vector = new DoubleWritable[vect.length];
        for(int i = 0; i< vect.length; i++){
            vector[i] = new DoubleWritable(vect[i]);
        }
        this.vector = vector;
    }


}