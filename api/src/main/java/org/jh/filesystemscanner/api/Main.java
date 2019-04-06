package org.jh.filesystemscanner.api;

import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.jh.filesystemscanner.core.FileInfo;
import org.jh.filesystemscanner.core.FilesDAO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Main {

    private static final HashFunction HASH_FUNCTION = Hashing.sha512();

    private static Logger LOG = LoggerFactory.getLogger(Main.class.getName());

    public static void main(String[] args) throws Exception {

        //        testComputingMD5LargeFiles();

        //        if(true) return;

        //        System.setProperty("derby.stream.error.logSeverityLevel", "0");
        //        System.setProperty("derby.language.logStatementText", "true");


        final long currentTime65 = System.currentTimeMillis();

        File dir = new File("/Users/hujol/Pictures/JO");
        //        File dir = new File("/Users/hujol/Pictures/Fan Pier/");
        //        File dir = new File("/Users/hujol/Pictures/Neto-B-Party");

        if(!dir.isDirectory()) {
            LOG.warn(String.format("Directory '%s' does not exist.", dir.getAbsoluteFile()));
            return;
        }
        FilesDAO dao = new FilesDAO();
        //        if(true) return;

        try {
            final long currentTime69 = System.currentTimeMillis();
            indexDirectory(dao, dir);
            final long delta69 = System.currentTimeMillis() - currentTime69;
            LOG.info("**** Executed  in " + new DecimalFormat("0.0000").format(delta69 / 1000f) + "s");

/*
            final List<FileInfo> fileInfos = dao.getFileInfos();
            LOG.info("fileInfos.size() = " + fileInfos.size());

            final StringBuilder sb = new StringBuilder();
            final ImmutableMap<Object, FileInfo> hashToFileInfo = Maps.uniqueIndex(fileInfos,
                    fileInfo -> null == fileInfo ? "NO MD5 HASH" : fileInfo.getHashDigest());
            for(Map.Entry<Object, FileInfo> entry : hashToFileInfo.entrySet()) {
                sb.delete(0, sb.length());
                sb.append(entry.getKey());
                sb.append(" -- ");
                sb.append(entry.getValue());
                LOG.info(sb.toString());
            }
*/
        } finally {
            dao.close();
        }

        final long delta65 = System.currentTimeMillis() - currentTime65;
        LOG.info("**** Executed main in " + new DecimalFormat("0.0000").format(delta65 / 1000f) + "s");
        System.exit(0);
    }

    public static void indexDirectory(final FilesDAO dao, final File directory) throws Exception {
        final Executor executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        final BlockingQueue<Object[]> queue = new ArrayBlockingQueue<>(20);
        final AtomicInteger nbFiles = new AtomicInteger(0);

        // Start the producer.
        final CompletionService completionService = new ExecutorCompletionService(Executors.newFixedThreadPool(1));
        final Callable<Map<String, File>> runnable = () -> scanFiles(directory, Maps.newHashMap());
        final long currentTime105 = System.currentTimeMillis();
        completionService.submit(runnable);

/*
        // Processing files put in the queue as they come along.
        while(true) {
            Object[] objects = null;
            try {
                objects = queue.poll(250, TimeUnit.MILLISECONDS);
                if(0 == nbFiles.get()) {
                    break;
                }
            } catch(InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
            if(null != objects) {
                final File finalFile = (File) objects[0];
                System.out.println("  unqueued " + (finalFile).getAbsolutePath());

                final Object[] finalObjects = objects;
                executor.execute(() -> {
                    LOG.info("{} processing {}", Thread.currentThread().getName(), finalFile.getAbsolutePath());
                    try {
                        processFile(dao, finalFile, (Boolean) finalObjects[1]);
                    } catch(Exception e) {
                        LOG.error("Failed to ");
                        e.printStackTrace();
                    } finally {
                        nbFiles.decrementAndGet();
                    }
                });
            }
        }
*/

        // Wait that the scan finishes.
        try {
            Map<String, File> pathToFile = (Map<String, File>) completionService.take().get();

            LOG.info("Scanned {} files from directory {}", pathToFile.size(), directory.getAbsolutePath());
            // Check that all files in the queue have been processed.
            while(0 != nbFiles.get()) {
                Thread.currentThread().wait(250);
            }
        } catch(InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        final long delta105 = System.currentTimeMillis() - currentTime105;
        LOG.info("**** Executed scanFiles in " + new DecimalFormat("0.0000").format(delta105 / 1000f) + "s");


        LOG.info("Done indexing directory " + directory.getAbsolutePath());
    }

    private static Map<String, File> scanFiles(File directory, Map<String, File> filesToCheck) {
        LOG.debug("Visiting directory {}", directory);
        final File[] files = directory.listFiles();
        if(null == files) {
            return Collections.emptyMap();
        }

        // Visit in depth-first the file system.
        // Record the files to be treated.
//        while(true) {
//            Collection<File> dirsVisited = new ArrayList<>();
        for(File file : files) {
            if(file.isDirectory() && file.canRead()) {
                scanFiles(file, filesToCheck);
            } else {
                filesToCheck.put(file.getAbsolutePath(), file);
            }
//            }
        }

        return filesToCheck;
    }

    private static void visitDirectory(FilesDAO dao, File directory, BlockingQueue<Object[]> queue, AtomicInteger nbFiles)
            throws SQLException {
        LOG.info("visiting " + directory.getAbsolutePath());

        final Map<String, File> filesToCheck = scanFiles(directory, Maps.newHashMap());

        // All paths that need to be checked if they're already treated.
        final List<String> filePaths = Lists.newArrayList(filesToCheck.keySet());

        // Search the file information.
        final List<FileInfo> filesFound = dao.getFileInfos(filePaths);
        final Collection<String> filePathsNew;
        final Collection<String> filePathsToCheckHash;

        if(filesFound.isEmpty()) {
            filePathsNew = Lists.newArrayList(filePaths);
            filePathsToCheckHash = Lists.newArrayList();
        } else {
            // Create a map from path to file info.
            final Map<String, FileInfo> pathFoundToFileInfo =
                    Maps.uniqueIndex(filesFound, fileInfo -> null == fileInfo ? "" : fileInfo.getPath());

            // if the time from DB is different from to check then keep.

            // Get the paths.
            final List<String> filePathsFound = filesFound.stream()
                    .map(fileInfo -> null == fileInfo ? "" : fileInfo.getPath()).collect(Collectors.toList());

            // Filter out the files that need to be treated because they are new.
            filePathsNew = Collections2.filter(filePaths, s -> null != s && !filePathsFound.contains(s));

            // Filter out the ones that need to be treated because it was modified since last check.
            filePathsToCheckHash = Collections2.filter(filePathsFound, s -> {
                File file = filesToCheck.get(s);
                return null != s && null != file && pathFoundToFileInfo.get(s).getTimeComputed().getTime() != file.lastModified();
            });
        }

        // Queue the new files.
        for(String filePath : filePathsNew) {
            try {
                final File file = filesToCheck.get(filePath);
                queue.put(new Object[]{file, false});
                nbFiles.incrementAndGet();
                LOG.info("  submitted " + file.getAbsolutePath());
            } catch(InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }

        // Queue the files to check the hash.
        for(String filePath : filePathsToCheckHash) {
            try {
                final File file = filesToCheck.get(filePath);
                queue.put(new Object[]{file, true});
                nbFiles.incrementAndGet();
                LOG.info("  submitted " + file.getAbsolutePath());
            } catch(InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }

        LOG.info("Done visiting " + directory.getAbsolutePath());
    }

    private static void processFile(FilesDAO dao, File file, Boolean checkHash) throws SQLException, IOException {
        FileInputStream fin = null;
        try {
            fin = new FileInputStream(file);
            byte[] data = new byte[(int) file.length()];
            fin.read(data);
            fin.close();
            String hash = HASH_FUNCTION.hashBytes(data).toString();

            BasicFileAttributes attr = Files.readAttributes(file.toPath(), BasicFileAttributes.class);
            dao.insertFile(new FileInfo[]{new FileInfo(hash, file.getAbsolutePath(), new Timestamp(attr.creationTime().toMillis()), attr.size())});
        } finally {
            if(null != fin) {
                try {
                    fin.close();
                } catch(IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private static String hashFile(File file) throws NoSuchAlgorithmException, IOException {
        MessageDigest md = MessageDigest.getInstance("md5");
        try(InputStream in = new FileInputStream(file)) {
            byte[] bytes = new byte[8092];
            for(int n; -1 != (n = in.read(bytes)); ) {
                md.update(bytes, 0, n);
            }
        }
        final byte[] digest = md.digest();
        final StringBuilder sb = new StringBuilder();
        for(byte b : digest) {
            // Converting binary in big Indian then hexadecimal.
            sb.append(Integer.toString((b & 0xff) + 0x100, 16).substring(1));
        }
        return sb.toString();
    }

    private static void testComputingMD5LargeFiles() throws IOException, NoSuchAlgorithmException {
        final String[] filePaths = {
                "/Users/hujol/Pictures/2011-12-Taiba/2011-12-27/GOPR0347.MP4",
                "/Users/hujol/Pictures/2011-12-Taiba/2011-12-27/GOPR0334.MP4",
                "/Users/hujol/Pictures/2011-12-Taiba/2011-12-19/GOPR0301.MP4",
                "/Users/hujol/Pictures/2011-12-Taiba/2011-12-08/GOPR0137.MP4",
                "/Users/hujol/Pictures/2011-12-Taiba/2011-12-04/GOPR0077.MP4"
        };
        for(String filePath : filePaths) {
            final File file = new File(filePath);
            final String hashB = hashFile(file);
            LOG.info(hashB + " - " + filePath + " - " + file.length());
        }
    }
}
