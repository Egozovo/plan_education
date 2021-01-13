package ru.education.sgugit_java;

import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;

public class ConnectDB
{
    final String url = "jdbc:postgresql://localhost:5432/postgres";
    final String user = "postgres";
    final String password = "postgres";
     Connection con;
    Statement statement;

    public ConnectDB(){
        try{
            Class.forName("org.postgresql.Driver");
            con = DriverManager.getConnection(url, user, password);
            statement = con.createStatement();
        }catch (SQLException | ClassNotFoundException e){
            System.out.println("Connection failed...");
            e.printStackTrace();
        }
}



    //Работа с БД
    public void workDB (String fileExcel) throws SQLException {

        DatabaseMetaData dbm = con.getMetaData();
        ResultSet tables = dbm.getTables(null,null, "temp_plan_ed", null);   //если таблица есть, то происходит удаление и создание новой
        if(tables.next()){
            System.out.println("Таблица создана ранее. Вычищается");
            statement.execute("DROP TABLE IF EXISTS  temp_plan_ed; " +
                    "DROP TABLE IF EXISTS  plan1; " +
                    "DROP TABLE IF EXISTS  plan2; " +
                    "DROP TABLE IF EXISTS  plan3; " +
                    "DROP TABLE IF EXISTS plan_prepod_null;" +
                    "DROP TABLE IF EXISTS groupe_plan_ed;" +
                    "DROP TABLE IF EXISTS final_plan_ed;" +
                    "DROP TABLE IF EXISTS plan_ed_old_sem");
            statement.execute("CREATE TABLE public.temp_plan_ed " +
                    "(course integer, sem_number integer, name_discipline character varying," +
                    "discipline integer, id_form character varying, id_spec integer, napravlenie character varying, " +
                    "specialty character varying, name_profile character varying," +
                    "lector character varying, snils character varying, lec1 character varying, snils1 character varying," +
                    "lec2 character varying, snils2 character varying, lec3 character varying, snils3 character varying," +
                    "year character varying, cafedra character varying)");
        }else {
            //создает временную таблицу с планом если ее нет
            try {
                statement.executeUpdate("CREATE TABLE public.temp_plan_ed (course integer, sem_number integer, name_discipline character varying, " +
                        "discipline integer, id_form character varying, id_spec integer, napravlenie character varying, " +
                        "specialty character varying, name_profile character varying, " +
                        "lector character varying, snils character varying, lec1 character varying, snils1 character varying, " +
                        "lec2 character varying, snils2 character varying, lec3 character varying, snils3 character varying, " +
                        "year character varying, cafedra character varying)");
                System.out.println("Таблица создана.");
            } catch (SQLException e) {
                System.out.println("Connection failed...");
                e.printStackTrace();
            }
        }

        try{
            //загружаем данные CSV в таблицу
            System.out.println("Копирование данных в таблицу");
            CopyManager cp = new CopyManager((BaseConnection) con);
            cp.copyIn("COPY temp_plan_ed FROM STDIN WITH CSV HEADER DELIMITER ';';", new BufferedReader(new FileReader(fileExcel)));

        //удаляем лишние колонки

            System.out.println("Удаление лишних полей");
            statement.execute("ALTER TABLE temp_plan_ed DROP COLUMN name_discipline, DROP COLUMN napravlenie, "
            + "DROP COLUMN specialty, DROP COLUMN name_profile, DROP COLUMN year, DROP COLUMN cafedra");

        //обновить записи в коды

            System.out.println("Обновление формы обучения");
            statement.executeUpdate("UPDATE temp_plan_ed SET id_form='1' WHERE id_form='Заочная'; " +
                    "UPDATE temp_plan_ed SET id_form='2' WHERE id_form='Заочная сокращенная';" +
                    "UPDATE temp_plan_ed SET id_form='3' WHERE id_form='Заочная ускоренная'; " +
                    "UPDATE temp_plan_ed SET id_form='4' WHERE id_form='Очная';" +
                    "UPDATE temp_plan_ed SET id_form='5' WHERE id_form='Очно-заочная'; " +
                    "UPDATE temp_plan_ed SET id_form='6' WHERE id_form='Очно-заочная ускоренная'; ");

//обновляем семестры
            System.out.println("Обновление семестра");
            statement.executeUpdate("UPDATE temp_plan_ed SET sem_number=1 WHERE sem_number IS NULL AND course=1;" +
                    "UPDATE temp_plan_ed SET sem_number=2 WHERE sem_number IS NULL AND course=2;" +
                    "UPDATE temp_plan_ed SET sem_number=3 WHERE sem_number IS NULL AND course=3;" +
                    "UPDATE temp_plan_ed SET sem_number=4 WHERE sem_number IS NULL AND course=4;" +
                    "UPDATE temp_plan_ed SET sem_number=5 WHERE sem_number IS NULL AND course=5;" +
                    "UPDATE temp_plan_ed SET sem_number=6 WHERE sem_number IS NULL AND course=6;" +
                    "UPDATE temp_plan_ed SET sem_number=7 WHERE sem_number IS NULL AND course=7;" +
                    "UPDATE temp_plan_ed SET sem_number=8 WHERE sem_number IS NULL AND course=8;");

        //ищменяем тип колонки

            statement.execute("ALTER TABLE temp_plan_ed ALTER COLUMN id_form TYPE INT USING id_form::integer;");

//создаем копии таблиц

            statement.execute("SELECT course, sem_number, discipline, id_form, id_spec, lec1, snils1 INTO plan1 FROM temp_plan_ed;" +
                    "SELECT course, sem_number, discipline, id_form, id_spec, lec2, snils2 INTO plan2 FROM temp_plan_ed;" +
                    "SELECT course, sem_number, discipline, id_form, id_spec, lec3, snils3 INTO plan3 FROM temp_plan_ed;" +
                    "SELECT * INTO plan_prepod_null FROM temp_plan_ed WHERE lector IS NULL AND snils IS NULL AND lec1 IS NULL AND snils1 IS NULL " +
                    "AND lec2 IS NULL AND snils2 IS NULL AND lec3 IS NULL AND snils3 IS NULL;");

            statement.execute("ALTER TABLE temp_plan_ed DROP COLUMN lec1, DROP COLUMN snils1, DROP COLUMN lec2, DROP COLUMN snils2, " +
                    "DROP COLUMN lec3, DROP COLUMN snils3; " +
                    "ALTER TABLE plan_prepod_null DROP COLUMN lec1, DROP COLUMN snils1, DROP COLUMN lec2, DROP COLUMN snils2," +
                    "DROP COLUMN lec3, DROP COLUMN snils3;");

        //переименовываем колонки препопдов во временных таблицах
            statement.execute("ALTER TABLE temp_plan_ed RENAME COLUMN lector  TO pr;" +
                    "ALTER TABLE temp_plan_ed RENAME COLUMN snils TO id_teacher;" +
                    "ALTER TABLE plan1 RENAME COLUMN lec1 TO pr;" +
                    "ALTER TABLE plan1 RENAME COLUMN snils1 TO id_teacher;" +
                    "ALTER TABLE plan2 RENAME COLUMN lec2 TO pr;" +
                    "ALTER TABLE plan2 RENAME COLUMN snils2 TO id_teacher;" +
                    "ALTER TABLE plan3 RENAME COLUMN lec3 TO pr;" +
                    "ALTER TABLE plan3 RENAME COLUMN snils3 TO id_teacher;" +
                    "ALTER TABLE plan_prepod_null RENAME COLUMN lector TO pr;" +
                    "ALTER TABLE plan_prepod_null RENAME COLUMN snils TO id_teacher;");

        //соединяем таблицы

            statement.execute("INSERT INTO temp_plan_ed SELECT * FROM plan1;" +
                    "INSERT INTO temp_plan_ed SELECT * FROM plan2;" +
                    "INSERT INTO temp_plan_ed SELECT * FROM plan3;");

        //удаляем лишние строки, где преподы null

            statement.execute("DELETE FROM temp_plan_ed WHERE pr IS NULL;");


        //добавляем во временный план таблицу с null преподами

            statement.execute("INSERT INTO temp_plan_ed SELECT * FROM plan_prepod_null;" +
                    "ALTER TABLE temp_plan_ed DROP COLUMN pr");


        //Создаем временную таблицу с группами, созданную из таблицы student

            statement.execute("SELECT DISTINCT group_st, course, form_tr, profile INTO groupe_plan_ed FROM student;");

        //Создается итоговая таблица final_plan_ed

            statement.execute("SELECT p.course, p.sem_number, p.discipline, p.id_form, gr.group_st, p.id_teacher, p.id_spec " +
                    "INTO final_plan_ed " +
                    "FROM temp_plan_ed AS p INNER JOIN groupe_plan_ed AS gr " +
                    "ON (p.course=gr.course) AND (p.id_form=gr.form_tr) AND (p.id_spec=gr.profile);" +
                    "ALTER TABLE final_plan_ed RENAME COLUMN group_st TO id_group;");


        //в отдульную таблицу копируем прошлый семестр из нынешнего плана обучения

            statement.execute("SELECT * INTO plan_ed_old_sem FROM plan_ed WHERE sem_number = 1 AND sem_number = 3 AND " +
                    "sem_number = 5 AND sem_number = 7 AND sem_number = 9 AND sem_number = 11 AND sem_number = 13 AND sem_number = 15;" +
                    "DELETE FROM plan_ed;");


        //добвляем данные в глявную таблицу  план обучения

            statement.execute("INSERT INTO plan_ed SELECT * FROM plan_ed_old_sem;" +
                    "INSERT INTO plan_ed SELECT * FROM final_plan_ed;");


        //удаляем дубликаты
            statement.execute("DELETE FROM plan_ed a USING plan_ed b " +
                    "WHERE a.course=b.course AND a.sem_number=b.sem_number AND a.discipline=b.discipline " +
                    "AND a.id_form=b.id_form AND a.id_group=b.id_group AND a.id_teacher=b.id_teacher " +
                    "AND a.id_spec=b.id_spec AND a.ctid>b.ctid;" +
                    "ALTER TABLE plan_ed ALTER COLUMN id_spec TYPE VARCHAR");


        //удаляем все временные таблицы
            statement.execute("DROP TABLE temp_plan_ed;" +
                    "DROP TABLE final_plan_ed;" +
                    "DROP TABLE groupe_plan_ed;" +
                    "DROP TABLE plan1;" +
                    "DROP TABLE plan2;" +
                    "DROP TABLE plan3;" +
                    "DROP TABLE plan_ed_old_sem;" +
                    "DROP TABLE plan_prepod_null;");
        }catch(SQLException | IOException e){
            e.printStackTrace();
        } finally {
            statement.close();
            con.close();
        }
    }

//    Метод обнолвения графика обучения
//    для использования необходимо указать в фргументе командной строки флаг -g

    public void grafik_ed(String fileGrafik) throws SQLException {

        // Создаем временную таблицу графика обучения и заливаем данные

        try {
            statement.execute("CREATE TABLE IF NOT EXISTS temp_grafik_ed" +
                    "(profile INT, form VARCHAR, course INT, sem INT, values_grfaik VARCHAR, date_start VARCHAR, date_end VARCHAR);");

            CopyManager cp = new CopyManager((BaseConnection) con);
            cp.copyIn("COPY temp_grafik_ed FROM STDIN WITH CSV HEADER DELIMITER ';';", new BufferedReader(new FileReader(fileGrafik)));

            //statement.execute("COPY temp_grafik_ed FROM " + "'" + fileGrafik + "'" + " WITH CSV HEADER DELIMITER ';';");

            System.out.println("Удаление лишних полей");
            statement.execute("ALTER TABLE temp_grafik_ed DROP COLUMN values_grfaik;");


            // Приведение формы обучения в кодовый вид
            statement.execute("UPDATE temp_grafik_ed SET form='1' WHERE form='Заочная';" +
                    "UPDATE temp_grafik_ed SET form='2' WHERE form='Заочная сокращенная';" +
                    "UPDATE temp_grafik_ed SET form='3' WHERE form='Заочная ускоренная';" +
                    "UPDATE temp_grafik_ed SET form='4' WHERE form='Очная';" +
                    "UPDATE temp_grafik_ed SET form='5' WHERE form='Очно-заочная';" +
                    "UPDATE temp_grafik_ed SET form='6' WHERE form='Очно-заочная ускоренная';");

            //Изменение типа данных колонки Формы обучения
            statement.execute("ALTER TABLE temp_grafik_ed ALTER COLUMN form TYPE INT USING form::integer;");

            statement.execute("UPDATE temp_grafik_ed SET sem=1 WHERE course=1 AND sem IS NULL;" +
                    "UPDATE temp_grafik_ed SET sem=2 WHERE course=2 AND sem IS NULL;" +
                    "UPDATE temp_grafik_ed SET sem=3 WHERE course=3 AND sem IS NULL;" +
                    "UPDATE temp_grafik_ed SET sem=4 WHERE course=4 AND sem IS NULL;" +
                    "UPDATE temp_grafik_ed SET sem=5 WHERE course=5 AND sem IS NULL;" +
                    "UPDATE temp_grafik_ed SET sem=6 WHERE course=6 AND sem IS NULL;" +
                    "UPDATE temp_grafik_ed SET sem=7 WHERE course=7 AND sem IS NULL;" +
                    "UPDATE temp_grafik_ed SET sem=8 WHERE course=8 AND sem IS NULL;");

            //Вставка данных в дефолтную таблицу графика обучения
            statement.execute("INSERT INTO grafik_ed(id_form, course, sem_number, date_start, date_end, id_profile) " +
                    "SELECT form, course, sem, date_start, date_end, profile FROM temp_grafik_ed;");

            statement.execute("DROP TABLE IF EXISTS temp_grafik_ed;");

            statement.execute("DELETE FROM grafik_ed a USING grafik_ed b " +
                    "WHERE a.id_form = b.id_form AND a.course = b.course AND a.sem_number = b.sem_number " +
                    "AND a.date_start = b.date_start AND a.date_end = b.date_end AND a.id_profile = b.id_profile AND a.ctid > b.ctid;");
        } catch (SQLException | IOException e) {
            e.printStackTrace();
        } finally {
            statement.close();
            con.close();
        }
    }

    public void addStudents(String fileStudent) {
        try {

            /*
            Если временная таблица существует, то удаляем ее
            Создаем новую временную таблицу
             */
            statement.execute("DROP TABLE IF EXISTS temp_student;" +
                    "CREATE TABLE IF NOT EXISTS temp_student (fio VARCHAR, email VARCHAR, st_book VARCHAR, inst_min VARCHAR, " +
                    "group_name VARCHAR, group_code INT, course INT, speciality_code VARCHAR, specialty_name VARCHAR, " +
                    "profile INT, profile_name VARCHAR, status VARCHAR, password VARCHAR, form VARCHAR, inst_max VARCHAR, " +
                    "number_up VARCHAR, date_up VARCHAR)");

            //Копируем данные во временную таблицу
            CopyManager cp = new CopyManager((BaseConnection) con);
            cp.copyIn("COPY temp_student FROM STDIN WITH CSV HEADER DELIMITER ';';", new BufferedReader(new FileReader(fileStudent)));

            //удаление ненужных полей
            statement.execute("ALTER TABLE temp_student DROP COLUMN group_name, DROP COLUMN speciality_code," +
                    "DROP COLUMN specialty_name, DROP COLUMN profile_name, DROP COLUMN inst_max, " +
                    "DROP COLUMN number_up, DROP COLUMN date_up;");

            statement.execute("ALTER TABLE temp_student ADD COLUMN id_spec VARCHAR;" +
                    "UPDATE temp_student SET id_spec=profile;");

            //Обновление формы обучения
            statement.execute("UPDATE temp_student SET form='1' WHERE form='Заочная';"+
                    "UPDATE temp_student SET form='2' WHERE form='Заочная сокращенная';" +
                    "UPDATE temp_student SET form='3' WHERE form='Заочная ускоренная';" +
                    "UPDATE temp_student SET form='4' WHERE form='Очная';" +
                    "UPDATE temp_student SET form='5' WHERE form='Очно-заочная';" +
                    "UPDATE temp_student SET form='6' WHERE form='Очно-заочная ускоренная';");

            //Обление институтов
            statement.execute("UPDATE temp_student SET inst_min='5' WHERE inst_min='Аспирантура';" +
                    "UPDATE temp_student SET inst_min='1' WHERE inst_min='ИГиМ';" +
                    "UPDATE temp_student SET inst_min='2' WHERE inst_min='ИКиП';" +
                    "UPDATE temp_student SET inst_min='6' WHERE inst_min='Техникум';" +
                    "UPDATE temp_student SET inst_min='4' WHERE inst_min='ИОиТИБ';");

            //Обновление статусов
            statement.execute("UPDATE temp_student SET status='1' WHERE status LIKE 'Учитс%' OR status LIKE 'Отпуск%';" +
                    "UPDATE temp_student SET status='3' WHERE status LIKE 'Акад%';");

            //Вычищение таблицы студентов и заливка данных
            statement.execute("DELETE FROM student;");
            statement.execute("INSERT INTO student(fio, email, pass, st_book, institute, group_st, course, form_tr, " +
                    "profile, status, id_spec) " +
                    "SELECT fio, email, password, st_book, inst_min, group_code, course, form, profile, status, id_spec FROM temp_student;");

            statement.execute("DROP TABLE temp_student;");
        } catch (SQLException | IOException e) {
            e.printStackTrace();
        }
    }
}
