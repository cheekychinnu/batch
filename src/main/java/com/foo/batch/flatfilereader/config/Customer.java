package com.foo.batch.flatfilereader.config;

import org.springframework.batch.item.ResourceAware;
import org.springframework.core.io.Resource;

public class Customer implements ResourceAware {
    private String id;
    private String firstName;
    private String lastName;

    public Customer(String id, String firstName, String lastName) {
        this.id = id;
        this.firstName = firstName;
        this.lastName = lastName;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    @Override
    public String toString() {
        return "Customer{" +
                "id='" + id + '\'' +
                ", firstName='" + firstName + '\'' +
                ", lastName='" + lastName + '\'' +
                ", resouce='"+resource+"'"+
                '}';
    }

    private Resource resource;

    @Override
    public void setResource(Resource resource) {
        this.resource = resource;
    }
}
