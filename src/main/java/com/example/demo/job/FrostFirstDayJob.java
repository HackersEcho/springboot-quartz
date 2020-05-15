package com.example.demo.job;

import com.example.demo.dataConversion.climate.FrostFirstDay;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

/**
 * @description:
 * @author: echo
 * @createDate: 2020/3/27
 * @version: 1.0
 */
public class FrostFirstDayJob implements BaseJob {
    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        new FrostFirstDay().init();
    }
}
