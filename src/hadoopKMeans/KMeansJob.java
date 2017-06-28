package hadoopKMeans;

import hadoopDataModel.Centroid;
import hadoopDataModel.Point;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by Boris on 27/04/2017.
 */

public class KMeansJob extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new KMeansJob(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        int numIteration = 0;
        Configuration conf = getConf();
        conf.set("num_iteration", numIteration + "");
        Path inputPath = new Path(args[0]);
        Path outputRootPath = new Path(args[1]);
        String centersRootPath = args[2];//("centers");
        int numCentroids = Integer.parseInt(args[3]);
        FileSystem fs = FileSystem.get(outputRootPath.toUri(), conf);
        boolean success;
        long counter;
        int maxIterNum = 200;
        boolean maxIterNumReached = false;
        //int numCentroids = 3;
        if (fs.exists(outputRootPath)) {
            fs.delete(outputRootPath, true);
        }
        long startTime = System.nanoTime();
        do {
            conf.set("num_centroids", numCentroids + "");
            conf.set("centroids_path_toRead", centersRootPath + "/centers_" + numIteration);
            conf.set("centroids_path_toWrite", centersRootPath + "/centers_" + (numIteration + 1));

            Path outputPath = new Path(outputRootPath.toString() + "/iteraz_" + numIteration);
            Job job = Job.getInstance(conf, "KmeansClustering_" + numIteration);
            job.setJarByClass(KMeansJob.class);
            job.setMapperClass(KMeansMapper.class);
            job.setReducerClass(KMeansReducer.class);
            job.setNumReduceTasks(2);

            FileInputFormat.addInputPath(job, inputPath);
            FileOutputFormat.setOutputPath(job, outputPath);
            job.setOutputKeyClass(Centroid.class);
            job.setOutputValueClass(Point.class);
            success = job.waitForCompletion(true);
            numIteration++;

            if (numIteration > maxIterNum) {
                success = false;
                maxIterNumReached = true;
            }
            counter = job.getCounters().findCounter(KMeansReducer.Counter.CONVERGED).getValue();
        }

        while (counter < numCentroids && !maxIterNumReached);

        long endTime = System.nanoTime();
        long duration = (endTime - startTime)/1000000;
        System.out.println("tempo impiegato: " + duration);
        return success ? 0 : 1;
    }
}
