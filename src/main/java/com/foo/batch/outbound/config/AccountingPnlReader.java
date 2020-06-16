package com.foo.batch.outbound.config;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Component
@StepScope
public class AccountingPnlReader implements ItemReader<String> {
    private Iterator<String> iterator;

    public AccountingPnlReader() {
        List<String> input = IntStream.range(1, 6).mapToObj(i -> "portfolio" + i).collect(Collectors.toList());
        iterator = input.iterator();
    }

    @Autowired
    private AccountingPnlParameter accountingPnlParameter;

    @Override
    public String read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
        System.out.println(accountingPnlParameter.getParameter());
        if (iterator.hasNext()) {
            return iterator.next();
        }
        return null;
    }

}
