package Utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Scanner;

/**
 * Created by Boris on 27/04/2017.
 */
public final class InputFileReader {

    public static final InputFileReader INPUT_FILE_READER = new InputFileReader();
    String filePath;
    ArrayList<Double[]> pointList = new ArrayList<>();

    public ArrayList<Double[]> getPointList() {
        return pointList;
    }

    public void printPointList(){
        for (Double[] vector:pointList) {

            for(int i=0; i<vector.length; i++){
                System.out.print(vector[i]);
                System.out.print(", ");
            }
            System.out.print("\n");
        }
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public void readInput() throws IOException{
        if (filePath==null){
            throw new IOException("no input path specified");
        }
        try {
            File file = new File(filePath);
            FileReader fileReader = new FileReader(file);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            String line;

            while ((line = bufferedReader.readLine()) != null) {
                String[] tmp = line.split("\\s+");
                Double[] tmpVector = new Double[tmp.length];
                for(int i=0; i<tmp.length; i++){
                    tmpVector[i] =Double.parseDouble(tmp[i]);

                }
                pointList.add(tmpVector);

            }
            fileReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        printPointList();
    }

}



