package org.example;

import com.opencsv.CSVWriter;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Collections;
import java.util.Random;

public class DatasetGenerate {

    static String getRandomChar(int start, int end)
    {
        String characters = "abcdefghijklmnopqrstuvxyz";
        int nameSize = (int) (Math.random()*(end-start) + start);
        StringBuilder sb = new StringBuilder(nameSize);

        for (int i = 0; i < nameSize; i++) {
            int index = (int)(characters.length() * Math.random());
            sb.append(characters.charAt(index));
        }
        return sb.toString();
    }

    static int getAge()
    {
        return (int) (Math.random() * 60 + 10);
    }

    static String getGender(int x)
    {
        if (x%2 == 0){
            return "male";
        }
        else{
            return "female";
        }
    }

    static int getCode(){
        return (int) (Math.random() * 10 + 1);
    }

    static float getSalary(){
        return (float) (Math.random() * 10000 + 100);
    }

    static float getTransTotal(){
        return (float) (Math.random() * 1000 + 10);
    }

    public static void main(String[] args) {
        try {
            FileWriter custfile = new FileWriter("CustomersData.csv");

            for (int i = 1; i <= 50000; i++) {
                String name = getRandomChar(10, 20);
                int age = getAge();
                String gender = getGender(i);
                int countrycode = getCode();
                float salary = getSalary();

                Customer cust = new Customer(i, name, age, gender, countrycode, salary);
                custfile.append(cust.toString());
            }
            custfile.flush();
            custfile.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            FileWriter transfile = new FileWriter("TransactionsData.csv");
            int i = 1;
            for (int j = 1; j <= 50000; j++) {
                for (int k=1; k <= 100; k++) {
                    float transTotal = getTransTotal();
                    int transNumItems = getCode();
                    String transDesc = getRandomChar(20, 50);

                    Transaction trans = new Transaction(i, j, transTotal, transNumItems, transDesc);
                    i = i + 1;
                    transfile.append(trans.toString());
                }
            }
            transfile.flush();
            transfile.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}