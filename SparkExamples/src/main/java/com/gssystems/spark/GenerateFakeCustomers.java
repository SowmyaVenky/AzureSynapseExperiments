package com.gssystems.spark;

import com.google.gson.Gson;

import java.io.BufferedWriter;
import java.io.FileWriter;

import com.github.javafaker.Faker;

public class GenerateFakeCustomers {
    private static final String NL = "\n";

    public static void main(String[] args) throws Exception {
        if( args == null || args.length != 2) {
            System.out.println("Please specify number of person records to generate and outfile name");
            System.exit(-1);
        }

        FileWriter fw = new FileWriter(args[1]);
        BufferedWriter bw = new BufferedWriter(fw);

        Faker faker = new Faker();
        Gson gs = new Gson();

        for( int x = 0 ; x < Integer.parseInt(args[0]); x++ ) {
            String firstName = faker.name().firstName();
            String lastName = faker.name().lastName();

            String streetName = faker.address().streetName();
            String number = faker.address().buildingNumber();
            String city = faker.address().city();
            String state = faker.address().state();        
            String zip = faker.address().zipCode();

            String phone = faker.phoneNumber().cellPhone();
            String creditCard = faker.business().creditCardNumber();
            String expDate = faker.business().creditCardExpiry();
            String accountNumber = faker.commerce().promotionCode(10);

            Person p = new Person(  firstName, 
                                    lastName, 
                                    number, 
                                    streetName, 
                                    city, 
                                    state, 
                                    zip, phone, 
                                    creditCard, 
                                    expDate, 
                                    accountNumber);
            String personJson = gs.toJson(p);
            bw.write(personJson + NL);
            bw.flush();
        }

        bw.close();
    }
}
