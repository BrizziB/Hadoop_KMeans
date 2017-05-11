package hadoopKMeans;

import hadoopDataModel.Centroid;
import hadoopDataModel.Point;
import hadoopDataModel.VectorWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * Created by Boris on 27/04/2017.
 *Il mapper ed il reducer funzionano! Ho dovuto eliminare il Combiner che mi dava noia nell'output
 * adesso vanno fatti ciclare finchè non si stabilizzano.
 * ci sarà quindi da preparare una struttura decente di cartelle per permettere l'iterabilità
 */

public class KMeansJob extends Configured implements Tool{

    public static void main(String[] args)throws Exception{
        int res = ToolRunner.run(new Configuration(), new KMeansJob(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception{
        int numIteration = 0;

        Configuration conf = getConf();
        conf.set("num_iteration", numIteration + "");

        Path outputRootPath = new Path("output");
        Path inputPath = new Path("input");
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputRootPath)) {
            fs.delete(outputRootPath, true);
        }
        boolean success;
        long counter;
        int maxIterNum = 5;
        boolean maxIterNumReached = false;
        int numCentroids = 3;

        do{
            conf.set("centroids_path_toRead", "centers/centers_"+numIteration+".txt");
            conf.set("centroids_path_toWrite", "centers/centers_"+(numIteration+1)+".txt");
            Path outputPath = new Path(outputRootPath.toString()+"/iteraz_"+numIteration);

            Job job = Job.getInstance(conf, "KmeansClustering_"+numIteration);
            job.setJarByClass(KMeansJob.class);
            job.setMapperClass(KMeansMapper.class);
            job.setReducerClass(KMeansReducer.class);

            FileInputFormat.addInputPath(job, inputPath);
            FileOutputFormat.setOutputPath(job, outputPath);

            job.setOutputKeyClass(Centroid.class);
            job.setOutputValueClass(Point.class);

            success = job.waitForCompletion(true);
            numIteration++;

            if(maxIterNum > 5){
                success = false;
                maxIterNumReached=true;
            }

            counter = job.getCounters().findCounter(KMeansReducer.Counter.CONVERGED).getValue();


        }

        while (counter < numCentroids && !maxIterNumReached);
        return success? 0:1;
    }


    public int runOLD(String[] args) throws Exception{
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "KMeansClustering");

        job.setJarByClass(KMeansJob.class);
        job.setMapperClass(KMeansMapper.class);
        //job.setCombinerClass(KMeansReducer.class);
        job.setReducerClass(KMeansReducer.class);
/*
        job.setCombinerClass(IdentityReducer.class);
        job.setReducerClass(IdentityReducer.class);*/

        //job.setNumReduceTasks(3);

        //job.setInputFormatClass(SequenceFileInputFormat.class);


        job.setOutputKeyClass(Centroid.class);
        job.setOutputValueClass(Point.class);



        String centroidsPath = "centers/centers_0.txt";
        Path inputPath = new Path("input");
        Path centersPath = new Path(centroidsPath);
        Path outputPath = new Path ("output");
        conf.set("centroid.path", "centers/cen.seq");
        FileSystem fs = FileSystem.get(conf);

        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }
/*        if (fs.exists(centersPath)) {
            fs.delete(centersPath, true);
        }*/

        //writeExampleCenters(conf, centersPath, fs);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        boolean result = job.waitForCompletion(true);
        return result?1:0;
    }
}


/*

    private static final InputFileReader reader = new InputFileReader();

    private static final Logger LOG = LogManager.getLogger(KMeansJob.class);

    int numIterations = 0;
    Configuration conf = new Configuration();
        conf.set("num.iteration", numIterations+"");
                Path in = new Path("input");
                Path center = new Path("centers/cen.seq");
                conf.set("centroid.path", center.toString());
                Path out = new Path("depth_0");

                Job job = Job.getInstance(conf);
                job.setJobName("KMeans Clustering");

                job.setMapperClass(KMeansFirstTimeMapper.class);
        job.setReducerClass(KMeansReducer.class);
        job.setJarByClass(KMeansFirstTimeMapper.class);

        //serve per svuotare le cartelle di input output e dei centroidi se sono già state usate (se è presente un filesystem)
        FileInputFormat.addInputPath(job, in);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(out)) {
        fs.delete(out, true);
        }

        if (fs.exists(center)) {
        fs.delete(out, true);
        }

*/
/*        if (fs.exists(in)) {
            fs.delete(in, true);
        }*//*


        writeExampleCenters(conf, center, fs);

        FileOutputFormat.setOutputPath(job, out);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setOutputKeyClass(CentroidWritable.class);
        job.setOutputValueClass(PointWritable.class);

        job.waitForCompletion(true);

        long counter = job.getCounters().findCounter(KMeansReducer.Counter.CONVERGED).getValue();
        numIterations++;
        while (counter != 0) { //finchè non si ha la convergenza
        conf = new Configuration();
        conf.set("centroid.path", center.toString());
        conf.set("num.iteration", numIterations + "");
        job = Job.getInstance(conf);
        job.setJobName("KMeans Clustering " + numIterations);

        job.setMapperClass(KMeansMapper.class);
        job.setReducerClass(KMeansReducer.class);
        job.setJarByClass(KMeansMapper.class);

        in = new Path("depth_" + (numIterations - 1) + "/");
        out = new Path("depth_" + numIterations);

        FileInputFormat.addInputPath(job, in);
        if (fs.exists(out))
        fs.delete(out, true);

        FileOutputFormat.setOutputPath(job, out);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setOutputKeyClass(CentroidWritable.class);
        job.setOutputValueClass(PointWritable.class);

        job.waitForCompletion(true);
        numIterations++;
        counter = job.getCounters().findCounter(KMeansReducer.Counter.CONVERGED).getValue();
        }

@SuppressWarnings("deprecation")
        Path result = new Path("depth_" + (numIterations - 1) + "/");
                FileStatus[] stati = fs.listStatus(result);
                for (FileStatus status : stati) {
                if (!status.isDirectory()) {
                Path path = status.getPath();
                if (!path.getName().equals("_SUCCESS")) {
                LOG.info("FOUND " + path.toString());
                try (SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf)) {
                CentroidWritable key = new CentroidWritable();
                PointWritable v = new PointWritable();
                while (reader.next(key, v)) {
                LOG.info(key + " / " + v);

                }
                }
                }
                }
                }*/
