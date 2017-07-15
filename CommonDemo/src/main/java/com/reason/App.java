package com.reason;

import java.util.Arrays;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ){
        // main function
        System.out.println( "Hello World!" );
        lambda();
        DefaultFunctionInterface s = new DefaultFunctionInterface() {
            @Override
            public String defaultFunction() {
                return null;
            }
        };
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
