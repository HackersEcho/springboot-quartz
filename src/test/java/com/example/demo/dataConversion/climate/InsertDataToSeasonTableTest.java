package com.example.demo.dataConversion.climate;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.time.LocalDate;

@RunWith(SpringRunner.class)
@SpringBootTest
public class InsertDataToSeasonTableTest {

    @Test
    public void insertTotable() {
        LocalDate nowDate = LocalDate.now();
        int startYears = nowDate.getYear();
        int endYears = nowDate.plusDays(-1).getYear();
        InsertDataToSeasonTable.insertTotable(startYears, endYears);
    }
}
