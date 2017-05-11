package Utils;

import org.apache.hadoop.fs.Path;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Created by Boris on 08/05/2017.
 */
public final class FileLogger {

    public FileLogger(){}


    public static void printMapper(String content)throws IOException{
    return;
/*        Path outPath = new Path("centers/points.txt");
        FileWriter fileWriter = new FileWriter(outPath.toString(), true);
        BufferedWriter writer = new BufferedWriter(fileWriter);

        writer.write("Assegnazione di ogni punto al cluster il cui centroide è il più vicino");
        writer.newLine();
        writer.write(content);
        writer.newLine();
        writer.close();
        fileWriter.close();*/
    }

    public static void printReducer(String content) throws IOException{

        Path outPath = new Path("centers/points.txt");
        FileWriter fileWriter = new FileWriter(outPath.toString(), true);
        BufferedWriter writer = new BufferedWriter(fileWriter);

        writer.write("Nuove coordinate del centroide ");

        writer.write(content);
        writer.newLine();
        writer.close();
        fileWriter.close();
    }

    public static void printCleanup(int content)throws IOException{
        Path outPath = new Path("centers/centers_0.txt");
        FileWriter fileWriter = new FileWriter(outPath.toString(), true);
        BufferedWriter writer = new BufferedWriter(fileWriter);
        writer.newLine();
        writer.write(content);

        writer.close();
        fileWriter.close();
    }

    public static void printCleanup(String content, int num, String path)throws IOException{
        Path outPath = new Path(path);
        FileWriter fileWriter = new FileWriter(outPath.toString(), true);
        BufferedWriter writer = new BufferedWriter(fileWriter);
        writer.write(content);
        writer.newLine();
        writer.close();
        fileWriter.close();
    }

}
