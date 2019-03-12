package java.com.jh.fsduplicate;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

public final class FsDuplicate {

  private static final HashFunction HASH_FUNCTION = Hashing.md5();

  public static void indexDirectory(final FilesDAO dao, final File directory) throws Exception {
    final Executor executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    final BlockingQueue<Object[]> queue = new ArrayBlockingQueue<Object[]>(20);
    final AtomicInteger nbFiles = new AtomicInteger(0);

    // Start the producer.
    final CompletionService completionService = new ExecutorCompletionService(Executors.newFixedThreadPool(1));
    final Callable runnable = new Callable() {

      @Override
      public Object call() throws Exception {
        visitDirectory(dao, directory, queue, nbFiles);

        return null;
      }
    };
    completionService.submit(runnable);

loop:
    while(true) {
      Object[] objects = null;
      try {
        objects = queue.poll(250, TimeUnit.MILLISECONDS);
        if(0 == nbFiles.get()) {
          break;
        }
      }
      catch(InterruptedException ignored) {
        Thread.currentThread().interrupt();
      }
      if(null != objects) {
        final File finalFile = (File)objects[0];
        System.out.println("  unqueued " + (finalFile).getAbsolutePath());

        final Object[] finalObjects = objects;
        executor.execute(new Runnable() {
          @Override
          public void run() {
            System.out.println("  " + Thread.currentThread().getName() + " processing " + finalFile.getAbsolutePath());
            try {
              processFile(dao, finalFile, (Boolean)finalObjects[1]);
            }
            catch(Exception e) {
              System.err.println("Failed to ");
              e.printStackTrace();
            }
            finally {
              nbFiles.decrementAndGet();
            }
          }
        });
      }
    }

    try {
      completionService.take().get();
    }
    catch(InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    System.out.println("Done indexing directory " + directory.getAbsolutePath());
  }

  private static void visitDirectory(FilesDAO dao, File directory, BlockingQueue<Object[]> queue, AtomicInteger nbFiles)
      throws SQLException {
    System.out.println("visiting " + directory.getAbsolutePath());

    final File[] files = directory.listFiles();
    if(null == files) {
      return;
    }

    // Visit in depth-first the file system.
    // Record the files to be treated.
    final Map<String, File> filesToCheck = Maps.newHashMap();
    for(File file : files) {
      if(file.isDirectory() && file.canRead()) {
        visitDirectory(dao, file, queue, nbFiles);
      }
      else {
        filesToCheck.put(file.getAbsolutePath(), file);
      }
    }

    // All paths that need to be checked if they're already treated.
    final List<String> filePaths = Lists.newArrayList(filesToCheck.keySet());

    // Search the file information.
    final List<FileInfo> filesFound = dao.getFileInfos(filePaths);
    final Collection<String> filePathsNew;
    final Collection<String> filePathsToCheckHash;

    if(filesFound.isEmpty()) {
      filePathsNew = Lists.newArrayList(filePaths);
      filePathsToCheckHash = Lists.newArrayList();
    }
    else {
      // Create a map from path to file info.
      final Map<String, FileInfo> pathFoundToFileInfo = Maps.uniqueIndex(filesFound, new Function<FileInfo, String>() {
        @Override
        public String apply(@Nullable FileInfo fileInfo) {
          return null == fileInfo ? "" : fileInfo.getPath();
        }
      });

      // if the time from DB is different from to check then keep.

      // Get the paths.
      final List<String> filePathsFound = Lists.transform(filesFound, new Function<FileInfo, String>() {
        @Override
        public String apply(@Nullable FileInfo fileInfo) {
          return null == fileInfo ? "" : fileInfo.getPath();
        }
      });

      // Filter out the files that need to be treated because they are new.
      filePathsNew = Collections2.filter(filePaths, new Predicate<String>() {
        @Override
        public boolean apply(@Nullable String s) {
          return null != s && !filePathsFound.contains(s);
        }
      });

      // Filter out the ones that need to be treated because it was modified since last check.
      filePathsToCheckHash = Collections2.filter(filePathsFound, new Predicate<String>() {
        @Override
        public boolean apply(@Nullable String s) {
          File file = filesToCheck.get(s);
          return null != s && null != file && pathFoundToFileInfo.get(s).getTimeComputed().getTime() != file.lastModified();
        }
      });
    }

    // Queue the new files.
    for(String filePath : filePathsNew) {
      try {
        final File file = filesToCheck.get(filePath);
        queue.put(new Object[]{file, false});
        nbFiles.incrementAndGet();
        System.out.println("  submitted " + file.getAbsolutePath());
      }
      catch(InterruptedException ignored) {
        Thread.currentThread().interrupt();
      }
    }

    // Queue the files to check the hash.
    for(String filePath : filePathsToCheckHash) {
      try {
        final File file = filesToCheck.get(filePath);
        queue.put(new Object[]{file, true});
        nbFiles.incrementAndGet();
        System.out.println("  submitted " + file.getAbsolutePath());
      }
      catch(InterruptedException ignored) {
        Thread.currentThread().interrupt();
      }
    }

    System.out.println("Done visiting " + directory.getAbsolutePath());
  }

  private static void processFile(FilesDAO dao, File file, Boolean checkHash) throws SQLException, IOException {
    FileInputStream fin = null;
    try {
      fin = new FileInputStream(file);
      byte[] data = new byte[(int)file.length()];
      fin.read(data);
      fin.close();
      String hash = HASH_FUNCTION.hashBytes(data).toString();

      dao.insertFile(new FileInfo[]{new FileInfo(hash, file.getAbsolutePath(), new Timestamp(file.lastModified()))});
    }
    finally {
      if(null != fin) {
        try {
          fin.close();
        }
        catch(IOException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  private static String hashFile(File file) throws NoSuchAlgorithmException, IOException {
    MessageDigest md = MessageDigest.getInstance("md5");
    InputStream in = new FileInputStream(file);
    try {
      byte[] bytes = new byte[8092];
      for(int n; -1 != (n = in.read(bytes)); ) {
        md.update(bytes, 0, n);
      }
    }
    finally {
      in.close();
    }
    final byte[] digest = md.digest();
    final StringBuilder sb = new StringBuilder();
    for(int i = 0; i < digest.length; i++) {
      // Converting binary in big Indian then hexadecimal.
      sb.append(Integer.toString((digest[i] & 0xff) + 0x100, 16).substring(1));
    }
    return sb.toString();
  }

  public static void main(String[] args) throws Exception {

    //        testComputingMD5LargeFiles();

    //        if(true) return;

    //        System.setProperty("derby.stream.error.logSeverityLevel", "0");
    //        System.setProperty("derby.language.logStatementText", "true");


    final long currentTime65 = System.currentTimeMillis();

    File dir = new File("/Users/hujol/Documents/");
    //        File dir = new File("/Users/hujol/Pictures/Fan Pier/");
    //        File dir = new File("/Users/hujol/Pictures/Neto-B-Party");

    if(!dir.isDirectory()) {
      System.out.println(String.format("Directory '%s' does not exist.", dir.getAbsoluteFile()));
      return;
    }
    FilesDAO dao = new FilesDAO();
    //        if(true) return;

    final long currentTime75 = System.currentTimeMillis();
    indexDirectory(dao, dir);
    final long delta75 = System.currentTimeMillis() - currentTime75;
    System.out.println("**** Executed indexDirectory in " + new DecimalFormat("0.0000").format(delta75 / 1000f) + "s");


    //        final StringBuilder sb = new StringBuilder();
    final List<FileInfo> fileInfos = dao.getFileInfos();
    System.out.println("fileInfos.size() = " + fileInfos.size());
    //        final ImmutableMap<Object, FileInfo> hashToFileInfo = Maps.uniqueIndex(fileInfos, new Function<FileInfo, Object>() {
    //            @Override
    //            public Object apply(@Nullable FileInfo fileInfo) {
    //                return null == fileInfo ? "NO MD5 HASH" : fileInfo.getHashDigest();
    //            }
    //        });
    //        for(Map.Entry<Object, FileInfo> entry : hashToFileInfo.entrySet()) {
    //            sb.delete(0, sb.length());
    //            sb.append(entry.getKey());
    //            sb.append(" -- ");
    //            sb.append(entry.getValue());
    //            System.out.println(sb.toString());
    //        }
    dao.close();
    final long delta65 = System.currentTimeMillis() - currentTime65;
    System.out.println("**** Executed main in " + new DecimalFormat("0.0000").format(delta65 / 1000f) + "s");
    System.exit(0);
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
      System.out.println(hashB + " - " + filePath + " - " + file.length());
    }
  }
}
