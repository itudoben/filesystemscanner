package org.jh.filesystemscanner;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.io.Resources;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author <font size=-1 color="#a3a3a3">Johnny Hujol</font>
 * @since 5/5/12
 */
public final class FilesDAO {

    private static final String DRIVER = "org.h2.Driver";
    //    private static final String DB_NAME = "fsduplicatedb";
//    private static final String CONNECTION_URL = "jdbc:derby:" + DB_NAME + ";create=true";
    private static final String TABLE_NAME = "file";

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public FilesDAO() throws Exception {
        setDBSystemDir();
        loadDatabaseDriver(DRIVER);
        start();
    }

    private static boolean checkIfTableExists(Connection conTst, String tableName) throws SQLException {
        try {
            @SuppressWarnings({"JDBCResourceOpenedButNotSafelyClosed"})
            Statement s = conTst.createStatement();
            s.execute("SELECT count(*) FROM " + tableName);
        } catch(SQLException sqle) {
            String theError = sqle.getSQLState();
            /** If table exists will get -  WARNING 02000: No row was found **/
            if("42S02".equals(theError)) { // Table does not exist
                return false;
            } else if("42X14".equals(theError) || "42821".equals(theError)) {
                System.out.println("checkTable: Incorrect table definition. Drop table activity and rerun this program");
                throw sqle;
            } else {
                System.out.println("checkTable: Unhandled SQLException " + theError);
                throw sqle;
            }
        }
        return true;
    }

    private static Connection getConnection() throws SQLException {
        Connection connection = DriverManager.getConnection("jdbc:h2:~/.fsduplicate/db", "sa", "");
//        connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        return connection;
    }

    private static void closeNoFail(Statement statement) throws SQLException {
        if(null != statement) {
            statement.close();
        }
    }

    private static void closeNoFail(ResultSet resultSet) throws SQLException {
        if(null != resultSet) {
            resultSet.close();
        }
    }

    private static void closeNoFail(Connection con) throws SQLException {
        if(null != con) {
            con.close();
        }
    }

    private static void loadDatabaseDriver(String driverName) throws ClassNotFoundException {
        // Load the Java DB driver.
        Class.forName(driverName);
    }

    private void setDBSystemDir() {
        // Configure our Derby database
        final File dbDirFile = new File(System.getProperty("user.home"), ".fsduplicate");
        if(!dbDirFile.exists()) {
            Preconditions.checkArgument(dbDirFile.mkdir(), "Cannot create directory '" + dbDirFile.getAbsolutePath() + "'.");
        }

        // Set the db system directory.
        System.setProperty("derby.system.home", dbDirFile.getAbsolutePath());
        System.out.println(dbDirFile);
    }

    private void start() throws SQLException, IOException {
        final Connection conn = getConnection();
        if(!checkIfTableExists(conn, TABLE_NAME)) {
            System.out.println(" . . . . creating table " + TABLE_NAME);
            final String fileDdl = Resources.toString(Resources.getResource("file_ddl.sql"), Charset.forName("UTF-8"));
            Statement s = conn.createStatement();
            try {
                s.execute(fileDdl);
            } finally {
                closeNoFail(s);
            }
        }
    }

    public void insertFile(FileInfo[] fileInfos) throws SQLException {
        Connection con = null;
        PreparedStatement statement = null;
        lock.writeLock().lock();
        try {
            con = getConnection();
            statement = con.prepareStatement(
                    "INSERT INTO file" +
                            "   (md5_hash, path, extension, size, last_visited_time) " +
                            "VALUES (?, ?, ?, ?, ?)",
                    Statement.NO_GENERATED_KEYS);
            for(FileInfo fio : fileInfos) {
                statement.clearParameters();
                statement.setString(1, fio.getHashDigest());
                statement.setString(2, fio.getPath());
                statement.setString(3, fio.getExtension());
                statement.setLong(4, fio.getSize());
                statement.setTimestamp(5, fio.getTimeComputed());
                try {
                    statement.executeUpdate();
                } catch(Exception e) {
                    System.out.println(e);
                    // Likely it's a duplicate.
                    // Ignore.
                }
            }
        } finally {
            lock.writeLock().unlock();
            closeNoFail(statement);
            closeNoFail(con);
        }
    }

    public void close() {
        // release resources
        if("org.apache.derby.jdbc.EmbeddedDriver".equals(DRIVER)) {
            boolean gotSQLExc = false;
            try {
                DriverManager.getConnection("jdbc:derby:;shutdown=true");
            } catch(SQLException se) {
                if("XJ015".equals(se.getSQLState())) {
                    gotSQLExc = true;
                }
            }
            if(!gotSQLExc) {
                System.out.println("Database did not shut down normally");
            } else {
                System.out.println("Database shut down normally");
            }
        }
    }

    public List<FileInfo> getFileInfos() throws SQLException {
        return getFileInfos(Collections.emptyList());
    }

    public List<FileInfo> getFileInfos(Collection<String> filePaths) throws SQLException {
        final List<FileInfo> fileInfos = Lists.newArrayList();
        final boolean hasFilters = !filePaths.isEmpty();
        Joiner joiner = Joiner.on("','").skipNulls();
        final String condition = " WHERE path IN ('" + joiner.join(filePaths) + "')";

        final Connection con = getConnection();

        Statement st2 = null;
        ResultSet rs2 = null;
        lock.readLock().lock();
        try {
            st2 = con.createStatement();
            rs2 = st2.executeQuery("SELECT md5_hash, path, size, last_visited_time FROM file"
                    + (hasFilters ? condition : ""));
            while(rs2.next()) {
                final String md5Hash = rs2.getString("md5_hash");
                final String path = rs2.getString("path");
                final long size = rs2.getLong("size");
                final Timestamp time = rs2.getTimestamp("last_visited_time");
                fileInfos.add(new FileInfo(md5Hash, path, time, size));
            }
        } finally {
            lock.readLock().unlock();
            closeNoFail(st2);
            closeNoFail(rs2);
        }

        return fileInfos;
    }
}
