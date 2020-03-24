package com.example.demo.job;

import com.example.demo.dataConversion.climate.VoltDroughtDataTable;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

/**
 * @description:
 * @author: echo
 * @createDate: 2020/3/23
 * @version: 1.0
 */
public class VoltDroughtJob implements BaseJob{
    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        VoltDroughtDataTable dataTable = new VoltDroughtDataTable();
        dataTable.init();
    }
}
