package com.reason.service;

import com.reason.model.User;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

@Service
public class SampleService {

    private List<User> list = new ArrayList<User>();

    public String getHelloMessage(){
        return "Hello world.";
    }

    public void saveUser(User user) {
        list.add(user);
    }

    public List<User> findUser(){
        return list;
    }
}
