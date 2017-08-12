package com.reason.web;

import com.reason.model.User;
import com.reason.service.SampleService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

@RestController
public class SampleController {

    @Autowired
    private SampleService service;

    @GetMapping("/")
    public String helloWorld(){
        return service.getHelloMessage();
    }

    @RequestMapping(value = "/user/save" , method = RequestMethod.POST)
    @ResponseBody
    public Map<String, Object> saveUser(@RequestBody User user){
        Map<String, Object> response = new HashMap<>();
        response.put("code","S");
        System.out.println("User:"+user);
        service.saveUser(user);
        return response;
    }

    @GetMapping("/user/find")
    @ResponseBody
    public Map<String, Object> findUser(){
        Map<String, Object> response = new HashMap<>();
        response.put("code","S");
        response.put("data", service.findUser());
        return response;
    }


    @GetMapping("/async")
    public Callable<String> helloWorldAsync(){
        return new Callable<String>() {
            @Override
            public String call() throws Exception {
                return "async: "+ SampleController.this.service.getHelloMessage();
            }
        };
    }
}
