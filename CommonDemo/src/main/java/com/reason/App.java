package com.reason;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ){
        // main function
        System.out.println( "Hello World!" );
        //lambda();
        Set<String> ss = new HashSet<>();
        ss.add("ta");
        ss.add("tb");
        ss.add("tc");
        ss.add("dd");

        Iterator<String> it = ss.iterator();
        while(it.hasNext()){
            String s = it.next();
            System.out.println(s);
            if(s.startsWith("t")){
                it.remove();
                System.out.println(s.substring(1));
            }
        }
        //s.stream().filter(a -> a.startsWith("t")).forEach(a -> a = a.substring(1));
        System.out.println(ss);
    }

    // #1
    public static void lambda(){
        Arrays.asList("R","e","a","s","o","n").forEach( e -> System.out.print(e));
    }

    // #2
    public interface DefaultFunctionInterface{
        default String defaultFunction(){
            return "default function";
        }
    }
    // #2
    public interface staticFunctionInterface{
        static String staticFunction(){
            return "static function";
        }
    }



}
