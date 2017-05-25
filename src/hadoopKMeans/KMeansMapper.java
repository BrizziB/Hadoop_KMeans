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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Boris on 27/04/2017.
 */
public class KMeansMapper extends Mapper<LongWritable, Text, Centroid, Point> {

    private final List<Centroid> centroids = new ArrayList<>();
    private final List<Point> points = new ArrayList<>();
    private EuclideanDistance distanceMeasurer = EuclideanDistance.getEuclideanDistance();
    public static final Log log = LogFactory.getLog(KMeansMapper.class);
    @Override
    @SuppressWarnings("deprecation")
    protected void setup(Context context)throws IOException, InterruptedException{
        // leggo i centroidi da file e li salvo in centroids
        // vengono messi su file nel cleanup() del reducer
        super.setup(context);
        Configuration conf = context.getConfiguration();
        Path centers = new Path(conf.get("centroids_path_toRead"));//new Path("centers/centers_0.txt");//(conf.get("centroid.path")); // ("centers/cen.seq");//
/*        FileSystem fs = FileSystem.get(conf);
        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(centers)));
        String line;
        line=br.readLine(); //versione locale   */

        FileSystem fs = FileSystem.get(URI.create(centers.toString()), conf);
        FSDataInputStream fsInputStream = fs.open(centers);
        BufferedReader br = new BufferedReader(new InputStreamReader(fsInputStream));
        String line = br.readLine();//versione per EMR ma va bene anche in locale



        int index=0;
        int length;
        while(line != null) {
            String[] tmp = line.split("\\s+");
            length = tmp.length;
            DoubleWritable[] tmpVector = new DoubleWritable[length];
            for (int i = 0; i < length; i++) {
                tmpVector[i] = new DoubleWritable(Double.parseDouble(tmp[i]));
            }
            Centroid centroid = new Centroid(tmpVector, new IntWritable(index), index);
            this.centroids.add(centroid);
            index++;
            line = br.readLine();
        }
        br.close();
        fsInputStream.close();
        conf.set("num_centroids", ""+(centroids.size()));
    }
    @Override
    protected void map(LongWritable key, Text points, Context context)throws IOException, InterruptedException{
        //leggo i punti e li inserisco in points
        String[]lines = points.toString().split("\\r?\\n");//.split(System.getProperty("line.separator"));
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
