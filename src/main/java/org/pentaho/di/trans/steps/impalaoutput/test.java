package org.pentaho.di.trans.steps.impalaoutput;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;

public class test {


    public static void main(String[] args){
        String s1 ="2016/01/07 22:39:00.000000000".replace('/','-');
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String p_temp =formatter.format(Timestamp.valueOf(s1));
        System.out.println(Timestamp.valueOf(s1));
        System.out.println(p_temp);
    }
}
