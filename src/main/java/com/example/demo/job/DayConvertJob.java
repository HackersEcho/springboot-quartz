package com.example.demo.job;

import com.example.demo.dataConversion.DataEntranceSync;
import com.example.demo.dataConversion.DayToMonthNew;
import com.example.demo.dataConversion.DayToSeasonNew;
import com.example.demo.dataConversion.DayToYearNew;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @description:
 * @author: echo
 * @createDate: 2020/3/19
 * @version: 1.0
 */
public class DayConvertJob implements BaseJob{
    private static Logger _log = LoggerFactory.getLogger(DayConvertJob.class);
    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        DataEntranceSync.init(new DayToMonthNew());// 月数据转化同步
        DataEntranceSync.init(new DayToSeasonNew());
        DataEntranceSync.init(new DayToYearNew());
    }
}
