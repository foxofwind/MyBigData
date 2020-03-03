package com.yixin;

import java.io.File;

import com.yixin.watermark.WatermarkException;
import com.yixin.watermark.WatermarkProcessor;

/**
 * 测试环境http://192.168.177.104:8090/
 * Uat环境https://hydra-api.uat.yixincapital.com/
 * 生产环境https://hydra-base.yxqiche.com/
 * /hydra-api/api/watermark/makeTransparentWatermark
 * D:/image/water.png
 */
public class App {
    public static void main(String[] args) {
        File file = new File(args[0]);
        File imgFile = new File(args[1]);
        if (!file.exists()) {
            System.out.println("文件不存在!");
        }
        try {
            WatermarkProcessor.process(file, imgFile, 1);
        } catch (WatermarkException e) {
            e.printStackTrace();
        }
    }
}
