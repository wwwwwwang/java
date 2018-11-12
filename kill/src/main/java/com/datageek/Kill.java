package com.datageek;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class Kill {
    public static void main(String args[]) {
        String command = args[0];
        System.out.println("command = " + command);
        run(command);
    }

    public static void run(String command){
        Process process = null;
        List<String> processList = new ArrayList<String>();
        try {
            process = Runtime.getRuntime().exec(command);
            BufferedReader input = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line = "";
            while ((line = input.readLine()) != null) {
                processList.add(line);
            }
            input.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        for (String line : processList) {
            System.out.println(line);
        }
    }

}
