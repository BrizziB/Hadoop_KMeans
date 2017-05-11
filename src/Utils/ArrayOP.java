package Utils;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;

import java.lang.reflect.Array;

/**
 * Created by Boris on 03/05/2017.
 */
public final class ArrayOP {

    public static final ArrayOP ArrayMath = new ArrayOP();

    private ArrayOP(){

    }
    public DoubleWritable[] sumArrays (DoubleWritable[] add1, DoubleWritable[] add2){

        DoubleWritable[] result = new DoubleWritable[add1.length];
        for(int i=0; i<add1.length; i++){
            Double tmp = add1[i].get() + add2[i].get();
            result[i] = new DoubleWritable(tmp);

        }
        return result;
    }

    public IntWritable convertToIntWritableIndex(int index){
        return new IntWritable(index);
    }

    public String toString(IntWritable num){
        return num.toString();
    }

    public String toString(DoubleWritable num){
        return num.toString();
    }

    public String toString(DoubleWritable[] array){
        String string = "";
        for(int i=0; i<array.length; i++){
            string += toString(array[i]);
            string += " ";
        }

        return string;
    }

    public DoubleWritable subtractArrays (DoubleWritable[] base, DoubleWritable[] sott){
        Double tot=0.0;
        Double[] result = new Double[base.length];
        for(int i=0; i<base.length; i++){
            result[i] =(base[i].get()  - sott[i].get());
            tot+= Math.abs(result[i]);
        }
        return new DoubleWritable(tot/base.length);
    }

    public DoubleWritable[] divideBy (DoubleWritable[] divid , int num){
        DoubleWritable[] result = new DoubleWritable[divid.length];
        for(int i=0; i<divid.length; i++){
            result[i] = new DoubleWritable(divid[i].get()/num);

        }
        return result;
    }

}
