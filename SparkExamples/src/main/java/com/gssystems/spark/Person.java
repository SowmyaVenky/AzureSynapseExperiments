package com.gssystems.spark;

public class Person {
    private String firstName;
    private String lastName;
    private String streetNumber;
    private String streetName;
    private String city;
    private String state;
    private String zip;
    private String phoneNumber;
    private String creditCard;
    private String expDate;
    private String accountNumber;    

    public Person(String firstName, 
                  String lastName, 
                  String number, 
                  String streetName, 
                  String city, 
                  String state,
                  String zip, 
                  String phoneNumber, 
                  String creditCard, 
                  String expDate, 
                  String accountNumber) {
        this.firstName = firstName;
        this.lastName = lastName;
        this.streetNumber = number;
        this.streetName = streetName;
        this.city = city;
        this.state = state;
        this.zip = zip;
        this.phoneNumber = phoneNumber;
        this.creditCard = creditCard;
        this.expDate = expDate;
        this.accountNumber = accountNumber;
    }

    @Override
    public String toString() {
        return "Person [firstName=" + firstName + ", lastName=" + lastName + ", number=" + streetNumber + ", streetName="
                + streetName + ", city=" + city + ", state=" + state + ", zip=" + zip + ", phoneNumber=" + phoneNumber
                + ", creditCard=" + creditCard + ", expDate=" + expDate + ", accountNumber=" + accountNumber + "]";
    }

}
