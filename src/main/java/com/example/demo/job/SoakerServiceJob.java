package com.example.demo.job;

import com.example.demo.dataConversion.climate.SoakerService;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import java.time.Year;

/**
 * @description:
 * @author: echo
 * @createDate: 2020/3/27
 * @version: 1.0
 */
public class SoakerServiceJob implements BaseJob {
    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        String year = Year.now().toString();
        new SoakerService().getRainDateBYFisrtCondition(year);
    }
}
