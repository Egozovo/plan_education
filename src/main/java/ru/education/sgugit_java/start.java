package ru.education.sgugit_java;

public class start {
    public static void main(String[] args) throws Exception {

        String inputFile;

        ConnectDB db = new ConnectDB();
        db.connectDBPostgres();

        if (args.length==0){
            System.out.println("Нет аргументов...");
        }else {
            inputFile = args[0];
            db.workDB(inputFile);
        }
    }
}
