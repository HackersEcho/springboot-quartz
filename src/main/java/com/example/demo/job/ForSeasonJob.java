package com.example.demo.job;

import com.example.demo.dataConversion.climate.InsertDataToSeasonTable;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;

/**
 * @description:四季入季定时器
 * @author: echo
 * @createDate: 2020/3/20
 * @version: 1.0
 */
public class ForSeasonJob implements BaseJob{
    private static Logger _log = LoggerFactory.getLogger(DayConvertJob.class);
    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        LocalDate nowDate = LocalDate.now();
        int startYears = nowDate.plusYears(-1).getYear();
        int endYears = nowDate.getYear();
        _log.info(startYears+"-"+endYears+"四季入季数据同步");
        InsertDataToSeasonTable.insertTotable(startYears, endYears);
    }
}
