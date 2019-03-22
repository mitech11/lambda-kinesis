package com.practice.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.practice.lambda.model.Greet;

public class Hello implements RequestHandler<Greet, String> {
    public String handleRequest(Greet greet, Context context) {
        context.getLogger().log("Input: " + greet.getName());
        return "Hello World - " + greet.getName();
    }
}
