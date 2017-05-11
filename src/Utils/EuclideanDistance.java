package Utils;

import hadoopDataModel.Centroid;
import hadoopDataModel.Point;
import org.apache.hadoop.io.DoubleWritable;

/**
 * Created by Boris on 27/04/2017.
 */
public final class EuclideanDistance {

    public static final EuclideanDistance EUCLIDEAN_DISTANCE = new EuclideanDistance();

    private EuclideanDistance(){
        super();
    }

    public double measureDistance(DoubleWritable[] vect1, DoubleWritable[] vect2){

        double tot=0;
        int length = vect1.length;
        double diff;
        for(int i=0; i<length; i++){
            diff = vect1[i].get() - vect2[i].get();
            tot += diff*diff ;
        }
        tot = Math.sqrt(tot);
        return tot;

    }

    public double measureDistance(Point vect1, Centroid vect2){
        return measureDistance(vect1.getVector(), vect2.getVector());
    }

    public static EuclideanDistance getEuclideanDistance(){
        return EUCLIDEAN_DISTANCE;
    }

}
