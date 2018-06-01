package org.apache.phoenix.util.hive;



import com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.Callable;

/**
 * phoenix数据来源：hive表数据
 * @author yu.wang
 * @create 2018/5/30
 */
public class HiveSqlData implements Callable<HiveSqlData.HiveSqlResult>{

    private static final Log LOG = LogFactory.getLog(HiveSqlData.class);

    private static final String WORK_DIR_PREFIX="/alidata/data/phoenix/";
    private static final String EXECUTE_SQL_NAME="execute.hql";
    private static final String DATA_RESULT_NAME="result.csv";
    private String inputFile;

    /**
     * hive数据导出结果
     * @param inputFile
     */
    public HiveSqlData(String inputFile) {
        this.inputFile = inputFile;
    }

     static public class HiveSqlResult{
        private boolean isSuccess;
        private String filePath;

         public boolean isSuccess() {
             return isSuccess;
         }

         public void setSuccess(boolean success) {
             isSuccess = success;
         }

         public String getFilePath() {
             return filePath;
         }

         public void setFilePath(String filePath) {
             this.filePath = filePath;
         }

         @Override
         public String toString() {
             return isSuccess+","+filePath;
         }
     }

    @Override
    public HiveSqlResult call() {

        HiveSqlResult hiveSqlResult = new HiveSqlResult();

        String randomId = System.currentTimeMillis()+"_"+(new Random().nextInt((100-1))+1);

        File workDir = new File(WORK_DIR_PREFIX+randomId+"/");
        if(!workDir.exists()){
            workDir.mkdir();
        }

        try{

            String sql = FileUtils.readFileToString(new File(inputFile));

            //创建脚本
            FileUtils.write(new File(WORK_DIR_PREFIX+randomId+"/"+EXECUTE_SQL_NAME),sql);
            ArrayList commands = Lists.newArrayList("/bin/sh","-c","hive -f "+EXECUTE_SQL_NAME+ "| sed 's/[\\t]/,/g' > "
                + ""+DATA_RESULT_NAME);

            ProcessBuilder processBuilder = new ProcessBuilder(commands).directory(workDir);
            Process process = processBuilder.start();

            final BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()));

            new Thread(new Runnable() {
                String tempValue;
                @Override
                public void run() {
                    try{
                        while((tempValue=br.readLine())!=null){
                            System.out.println(tempValue);
                        }
                    }catch (IOException e){

                    }
                }
            });

            int exitCode = process.waitFor();

            if(exitCode==0){
                hiveSqlResult.setSuccess(true);
                hiveSqlResult.setFilePath(workDir.getAbsolutePath()+"/"+DATA_RESULT_NAME);
                System.out.println("hive表数据下载完成");
            }else {
                hiveSqlResult.setSuccess(false);
            }
        }catch (IOException e){
            LOG.info(e);
        }catch (InterruptedException e){
            
        }

        return hiveSqlResult;
    }
}
