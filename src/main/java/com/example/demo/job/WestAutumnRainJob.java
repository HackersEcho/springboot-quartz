package com.example.demo.job;

import com.example.demo.dataConversion.climate.WestAutumnRainService;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import java.sql.SQLException;
import java.time.Year;

/**
 * @description:
 * @author: echo
 * @createDate: 2020/3/23
 * @version: 1.0
 */
public class WestAutumnRainJob implements BaseJob{
    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        try {
            WestAutumnRainService.init(Year.now().toString());
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
