package ru.education.sgugit_java;

import java.util.Arrays;

public class start {

    public static void main(String[] args) throws Exception {

        String inputFile;
        String argP = "-p";
        String argG = "-g";
        String argS = "-s";

        ConnectDB db = new ConnectDB();

        if (args.length==0){
            System.out.println("Нет аргументов...");
            System.exit(1);
        }else if (args[0].equals(argP)) {
            inputFile = args[1];
            db.workDB(inputFile);
            System.out.println("План обучения обновлен");
        }else if (args[0].equals(argG)) {
            inputFile = args[1];
            db.grafik_ed(inputFile);
            System.out.println("График обучения обновлен");
        }else if(args[0].equals(argS)) {
            inputFile = args[1];
            db.addStudents(inputFile);
            System.out.println("Студенты обновлены");
        }
    }
}
