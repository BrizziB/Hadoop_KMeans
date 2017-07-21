package hadoopKMeans;

import Utils.ArrayOP;
import Utils.EuclideanDistance;
import Utils.FileLogger;
import hadoopDataModel.Centroid;
import hadoopDataModel.Point;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Boris on 27/04/2017.
 */
public class KMeansMapper extends Mapper<LongWritable, Text, Centroid, Point> {

    private final List<Centroid> centroids = new ArrayList<>();
    private EuclideanDistance distanceMeasurer = EuclideanDistance.getEuclideanDistance();
    @Override
    @SuppressWarnings("deprecation")
    protected void setup(Context context)throws IOException, InterruptedException{
        // leggo i centroidi da file e li salvo in centroids
        // vengono messi su file nel cleanup() del reducer
        super.setup(context);
        Configuration conf = context.getConfiguration();
        Path centers = new Path(conf.get("centroids_path_toRead"));//new Path("centers/centers_0_0.txt");//(conf.get("centroid.path")); // ("centers/cen.seq");//

        int numCentroids =Integer.parseInt(conf.get("num_centroids"));
        int index=0;
        for (int i=0; i<numCentroids; i++){
            try{
                FileSystem fs = FileSystem.get(URI.create(centers.toString() +"_"+i+".txt"), conf);
                FSDataInputStream fsInputStream = fs.open(new Path(centers.toString() +"_"+i+".txt"));
                BufferedReader br = new BufferedReader(new InputStreamReader(fsInputStream));
                String line = br.readLine();


                int length;
                while(line != null) {
                    String[] tmp = line.split("\\s+");
                    length = tmp.length;
                    DoubleWritable[] tmpVector = new DoubleWritable[length];
                    for (int j = 0; j < length; j++) {
                        tmpVector[j] = new DoubleWritable(Double.parseDouble(tmp[j]));
                    }
                    Centroid centroid = new Centroid(tmpVector, new IntWritable(index), index);
                    this.centroids.add(centroid);
                    index++;
                    line = br.readLine();
                }
                br.close();
                fsInputStream.close();
            }catch (IOException e){//un centroide è sparito perchè non aveva più punti
                Centroid centroid = new Centroid(new IntWritable(index), index);
                this.centroids.add(centroid);
                index++;
                context.getCounter(KMeansReducer.Counter.CONVERGED).increment(1);
            }
        }

    }
    @Override
    protected void map(LongWritable key, Text points, Context context)throws IOException, InterruptedException{
        //leggo i punti e li inserisco in points
        String[]lines = points.toString().split("\\r?\\n");
        int length;
        double minDist = Double.MAX_VALUE;
        String result ="\n";

        for (String line : lines) {
            int id = line.hashCode();
            String[] tmp = line.split("\\s+");
            length = tmp.length;
            Double[] tmpVector = new Double[length];
            for (int i = 0; i < length; i++) {
                tmpVector[i] = Double.parseDouble(tmp[i]);
            }
            Point point = new Point(tmpVector, id);
            Centroid closerCentroid = new Centroid();
            for (Centroid c : centroids) {
                double dist = distanceMeasurer.measureDistance(c.getVector(), point.getVector());
                if (closerCentroid == null) {
                    closerCentroid = c;
                    minDist = dist;
                } else {
                    if (minDist > dist) {
                        closerCentroid=c;
                        minDist = dist;
                    }
                }
            }
            point.setClusterID(closerCentroid.getClusterID());
            result += ("centroide numero: " + closerCentroid.getCentroidID() + " con coordinate " +
                    ArrayOP.ArrayMath.toString(closerCentroid.getVector()) +" ---> punto: " +  ArrayOP.ArrayMath.toString(point.getVector()) +"\n");
            FileLogger.printMapper(result);
            context.write(closerCentroid, point);//per ogni punto ritorna il centroide più vicino ed il punto stesso
        }
    }
}
