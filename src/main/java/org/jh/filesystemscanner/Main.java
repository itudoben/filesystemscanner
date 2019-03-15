package org.jh.filesystemscanner;

import java.io.File;

/**
 * @author hujol
 * @since 2019-03-14
 */
public final class Main {
    public static void main(String[] args) throws Exception {
        long length = new File("/Users/hujol/Projects/fsduplicate/fsduplicate_project/fsduplicate/settings.gradle").length();

        System.out.println(length);
    }
}
