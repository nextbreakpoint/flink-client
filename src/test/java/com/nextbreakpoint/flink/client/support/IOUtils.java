/*
 * This file is part of Flink Client
 * https://github.com/nextbreakpoint/flink-client
 */
package com.nextbreakpoint.flink.client.support;

import com.google.gson.Gson;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.io.BufferedInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class IOUtils {
    private static final Gson GSON = new Gson();

    public static void copyFile(String fileUrl, String filePath) {
        try {
            try (BufferedInputStream in = new BufferedInputStream(new URL(fileUrl).openStream());
                 FileOutputStream fileOutputStream = new FileOutputStream(filePath)) {
                byte[] bytes = new byte[1024];
                int length;
                while ((length = in.read(bytes, 0, 1024)) != -1) {
                    fileOutputStream.write(bytes, 0, length);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void dumpAsJson(Object object) {
        System.out.println(GSON.toJson(object));
    }
}
