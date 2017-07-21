package hadoopKMeans;

import Utils.ArrayOP;
import Utils.FileLogger;
import hadoopDataModel.Centroid;
import hadoopDataModel.Point;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Boris on 30/04/2017.
 */

public class KMeansReducer extends Reducer<Centroid, Point, Text, Text> {

    public static enum Counter {
        CONVERGED
    }

    private int counter=0;
    private final List<Centroid> centroids = new ArrayList<>();
    @Override
    //il reduce prende tutte le coppie con medesima key
    protected void reduce(Centroid key, Iterable<Point> values, Context context)throws IOException, InterruptedException{
        List<Point> pointList = new ArrayList<>();
        counter++;
        DoubleWritable[] newCenter = null;
        String result = "\n";
        Configuration conf = context.getConfiguration(); //metto nella cartella i nuovi centroidi
        boolean oldCentroid = false;
        for(Point pt : values){
            pointList.add( new Point(pt.getVector(), pt.getPointID()) );
            if (newCenter == null)
                newCenter = pt.getVector();
            else
                newCenter = ArrayOP.ArrayMath.sumArrays(newCenter, pt.getVector());
        }
        newCenter = ArrayOP.ArrayMath.divideBy(newCenter, pointList.size());
        Centroid centroidTmp = new Centroid(newCenter, key.getCentroidID(), key.getClusterID());
        centroids.add(centroidTmp);
        int iteraz=0;

        Path writeCentersPath = new Path(conf.get("centroids_path_toWrite")+"_"+key.getCentroidID()+".txt");
        FileSystem fs = FileSystem.get(URI.create(writeCentersPath.toString()), conf);
        if (fs.exists(writeCentersPath)) {
            fs.delete(writeCentersPath, true);
        }
        FSDataOutputStream fsDataOutputStream = fs.create(writeCentersPath);
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fsDataOutputStream));
        bw.write("");


        for (Point point : pointList){
            Text keyText;
            result+=(counter);
            if(iteraz==0){
                //write new centroid to next iteration centers file
                bw.write(ArrayOP.ArrayMath.toString(centroidTmp.getVector()));
                //casting centroid to text in order to be written to context
                keyText = new Text(ArrayOP.ArrayMath.toString(centroidTmp.getVector())+"\n\n");
            }
            else{
                keyText = new Text("");
            }
            bw.close();
            fsDataOutputStream.close();
            iteraz++;
            //casting points to text in order to be written to context
            Text valueText = new Text(ArrayOP.ArrayMath.toString(point.getVector()));
            context.write(keyText, valueText);
        }

        context.write(new Text(""), new Text(""));

        if (key.hasConverged(centroidTmp)){ //avvenuta convergenza
            context.getCounter(Counter.CONVERGED).increment(1);
        }
    }
}