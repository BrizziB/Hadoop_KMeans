package test;

import Utils.InputFileReader;

import java.io.IOException;

/**
 * Created by Boris on 30/04/2017.
 */
public class test {

    public static void main(String[] args){


        InputFileReader reader = new InputFileReader();
        reader.setFilePath("input/dataset_prova.txt");


        try {
            reader.readInput();
        }
        catch (IOException e){
            System.out.println("errore");
        }

    }

}
