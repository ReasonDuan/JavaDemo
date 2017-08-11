package com.reason.web;

import com.reason.service.SampleService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.Callable;

@RestController
public class SampleController {

    @Autowired
    private SampleService service;

    @GetMapping("/")
    public String helloWorld(){
        return service.getHelloMessage();
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
