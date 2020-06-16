package com.foo.batch.outbound.config;

import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.stereotype.Component;

@Component
@JobScope
public class AccountingPnlParameter {

    private String parameter;

    public String getParameter() {
        return parameter;
    }

    public void setParameter(String parameter) {
        System.out.println("Current parameter "+this.parameter);
        this.parameter = parameter;
    }
}
