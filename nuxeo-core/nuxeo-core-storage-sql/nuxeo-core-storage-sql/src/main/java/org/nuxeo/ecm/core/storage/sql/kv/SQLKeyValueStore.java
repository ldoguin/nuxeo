/*
 * (C) Copyright 2018 Nuxeo (http://nuxeo.com/) and others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Contributors:
 *     Florent Guillaume
 */
package org.nuxeo.ecm.core.storage.sql.kv;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.commons.lang3.StringUtils.defaultIfBlank;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

import java.io.Serializable;
import java.nio.charset.CharacterCodingException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.nuxeo.ecm.core.api.ConcurrentUpdateException;
import org.nuxeo.ecm.core.api.NuxeoException;
import org.nuxeo.ecm.core.storage.sql.ColumnType;
import org.nuxeo.ecm.core.storage.sql.jdbc.db.Column;
import org.nuxeo.ecm.core.storage.sql.jdbc.db.TableImpl;
import org.nuxeo.ecm.core.storage.sql.jdbc.dialect.Dialect;
import org.nuxeo.ecm.core.storage.sql.jdbc.dialect.DialectPostgreSQL;
import org.nuxeo.runtime.datasource.ConnectionHelper;
import org.nuxeo.runtime.kv.AbstractKeyValueStoreProvider;
import org.nuxeo.runtime.kv.KeyValueStoreDescriptor;
import org.nuxeo.runtime.transaction.TransactionHelper;

/**
 * SQL implementation of a Key/Value Store Provider.
 * <p>
 * The following configuration properties are available:
 * <ul>
 * <li>datasource: the datasource to use.
 * <li>table: the table to use. The default is the Store name.
 * </ul>
 * If a namespace is specified, it is used as a table name suffix, otherwise of the store name.
 *
 * @since 10.10
 */
public class SQLKeyValueStore extends AbstractKeyValueStoreProvider {

    private static final Logger log = LogManager.getLogger(SQLKeyValueStore.class);

    public static final String DATASOURCE_PROP = "datasource";

    public static final String TABLE_PROP = "table";

    /** Key column, a short string. */
    public static final String KEY_COL = "key";

    /** Long column, or NULL if the value is not representable as a Long. */
    public static final String LONG_COL = "long";

    /** String column, or NULL if the value is representable as a Long or not representable as a String. */
    public static final String STRING_COL = "string";

    /** Bytes column, or NULL if the value is representable as a Long or String. */
    public static final String BYTES_COL = "bytes";

    /** TTL column, holding expiration date in seconds since epoch, or NULL if there is no expiration. */
    public static final String TTL_COL = "ttl";

    protected static final int TTL_EXPIRATION_FREQUENCY_MS = 60_000; // 60 seconds

    protected static final int MAX_RETRY = 5;

    protected String dataSourceName;

    protected Dialect dialect;

    protected TableImpl table;

    protected Column keyCol;

    protected Column longCol;

    protected Column stringCol;

    protected Column bytesCol;

    protected Column ttlCol;

    protected Thread ttlThread;

    @Override
    public void initialize(KeyValueStoreDescriptor descriptor) {
        super.initialize(descriptor);
        Map<String, String> properties = descriptor.properties;
        dataSourceName = properties.get(DATASOURCE_PROP);
        if (StringUtils.isAllBlank(dataSourceName)) {
            throw new NuxeoException("Missing " + DATASOURCE_PROP + " property in configuration");
        }
        String tableProp = properties.get(TABLE_PROP);
        String namespace = descriptor.namespace;
        String tableName;
        if (isBlank(tableProp)) {
            tableName = defaultIfBlank(namespace, name).trim();
        } else {
            tableName = tableProp.trim();
            if (isNotBlank(namespace)) {
                tableName += "_" + namespace.trim();
            }
        }

        // check connection, get dialect and create table if needed
        try (Connection connection = getConnection()) {
            dialect = Dialect.createDialect(connection, null);
            getTable(connection, dialect, tableName);
        } catch (SQLException e) {
            throw new NuxeoException("Cannot get connection from datasource: " + dataSourceName, e);
        }
        startTTLThread();
    }

    @Override
    public void close() {
        stopTTLThread();
    }

    protected void startTTLThread() {
        ttlThread = new Thread(this::expireTTLThread);
        ttlThread.setName("Nuxeo-Expire-KeyValueStore-" + name);
        ttlThread.setDaemon(true);
        ttlThread.start();
    }

    protected Connection getConnection() throws SQLException {
        return ConnectionHelper.getConnection(dataSourceName);
    }

    protected void stopTTLThread() {
        if (ttlThread == null) {
            return;
        }
        ttlThread.interrupt();
        ttlThread = null;
    }

    /**
     * Runs in a thread to do TTL expiration.
     */
    protected void expireTTLThread() {
        log.debug("Starting TTL expiration thread for KeyValueStore: {}", name);
        try {
            for (;;) {
                if (Thread.currentThread().isInterrupted()) {
                    break;
                }
                Thread.sleep(TTL_EXPIRATION_FREQUENCY_MS);
                expireTTLOnce();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        log.debug("Stopping TTL expiration thread for KeyValueStore: {}", name);
    }

    /** A {@link java.util.function.Consumer Consumer} that can throw {@link SQLException}. */
    @FunctionalInterface
    protected interface SQLConsumer<T> {

        /**
         * Performs this operation on the given argument.
         *
         * @param t the input argument
         */
        void accept(T t) throws SQLException;
    }

    /** A {@link java.util.function.Function Function} that can throw {@link SQLException}. */
    @FunctionalInterface
    protected interface SQLFunction<T, R> {

        /**
         * Applies this function to the given argument.
         *
         * @param t the function argument
         * @return the function result
         */
        R apply(T t) throws SQLException;
    }

    protected void runWithConnection(SQLConsumer<Connection> consumer) {
        TransactionHelper.runWithoutTransaction(() -> {
            try (Connection connection = getConnection()) {
                consumer.accept(connection);
            } catch (SQLException e) {
                throw new NuxeoException(e);
            }
        });
    }

    protected <R> R runWithConnection(SQLFunction<Connection, R> function) {
        return TransactionHelper.runWithoutTransaction(() -> {
            try (Connection connection = getConnection()) {
                return function.apply(connection);
            } catch (SQLException e) {
                throw new NuxeoException(e);
            }
        });
    }

    protected void getTable(Connection connection, Dialect dialect, String tableName) throws SQLException {
        String tablePhysicalName = dialect.getTableName(tableName);
        table = new TableImpl(dialect, tablePhysicalName, tablePhysicalName);
        keyCol = addColumn(KEY_COL, ColumnType.SYSNAME);
        keyCol.setPrimary(true);
        keyCol.setNullable(false);
        longCol = addColumn(LONG_COL, ColumnType.LONG);
        stringCol = addColumn(STRING_COL, ColumnType.CLOB);
        bytesCol = addColumn(BYTES_COL, ColumnType.BLOB);
        ttlCol = addColumn(TTL_COL, ColumnType.LONG);
        table.addIndex(TTL_COL);
        if (!tableExists(connection)) {
            createTable(connection);
        }
    }

    protected Column addColumn(String columnName, ColumnType type) {
        String colPhysicalName = dialect.getColumnName(columnName);
        Column column = new Column(table, colPhysicalName, type, columnName);
        return table.addColumn(column.getKey(), column);
    }

    protected boolean tableExists(Connection connection) throws SQLException {
        DatabaseMetaData metadata = connection.getMetaData();
        String schemaName = null;
        if ("Oracle".equals(metadata.getDatabaseProductName())) {
            try (Statement st = connection.createStatement()) {
                String sql = "SELECT SYS_CONTEXT('USERENV', 'SESSION_USER') FROM DUAL";
                log.trace("SQL: {}", sql);
                try (ResultSet rs = st.executeQuery(sql)) {
                    rs.next();
                    schemaName = rs.getString(1);
                    log.trace("  -> schema: {}", schemaName);
                }
            }
        }
        String tablePhysicalName = table.getPhysicalName();
        try (ResultSet rs = metadata.getTables(null, schemaName, tablePhysicalName, new String[] { "TABLE" })) {
            boolean exists = rs.next();
            log.debug("Checking if table {} exists: {}", tablePhysicalName, exists);
            return exists;
        }
    }

    protected void createTable(Connection connection) throws SQLException {
        try (Statement st = connection.createStatement()) {
            String createSql = table.getCreateSql();
            log.trace("SQL: {}", createSql);
            st.execute(createSql);
            for (String sql : table.getPostCreateSqls(null)) {
                log.trace("SQL: {}", sql);
                st.execute(sql);
            }
        }
    }

    protected void expireTTLOnce() {
        runWithConnection(connection -> {
            try {
                String sql = "DELETE FROM " + table.getQuotedName() + " WHERE " + ttlCol.getQuotedName() + " < ?";
                try (PreparedStatement ps = connection.prepareStatement(sql)) {
                    Long ttlDeadline = getTTLValue(0);
                    ttlCol.setToPreparedStatement(ps, 1, ttlDeadline);
                    log.trace("SQL: {} deadline={}", sql, ttlDeadline);
                    int count = ps.executeUpdate();
                    log.trace("SQL: expired {} keys", count);
                }
            } catch (SQLException e) {
                if (dialect.isConcurrentUpdateException(e)) {
                    // ignore
                    return;
                }
                log.debug("Exception during TTL expiration", e);
            }
        });
    }

    @Override
    public void clear() {
        runWithConnection(connection -> {
            try (Statement st = connection.createStatement()) {
                String sql = "DELETE FROM " + table.getQuotedName();
                log.trace("SQL: {}", sql);
                st.execute(sql);
            }
        });
    }

    @Override
    public Stream<String> keyStream() {
        return runWithConnection((Connection connection) -> keyStream(connection, null));
    }

    @Override
    public Stream<String> keyStream(String prefix) {
        return runWithConnection((Connection connection) -> keyStream(connection, prefix));
    }

    protected Stream<String> keyStream(Connection connection, String prefix) throws SQLException {
        Column col = table.getColumn(KEY_COL);
        String sql = "SELECT " + col.getQuotedName() + " FROM " + table.getQuotedName();
        if (prefix != null) {
            sql += " WHERE " + col.getQuotedName() + " LIKE ?";
            String esc = dialect.getLikeEscaping();
            if (esc != null) {
                sql += esc;
            }
        }
        log.trace("SQL: {}", sql);
        List<String> keys = new ArrayList<>();
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            if (prefix != null) {
                String like = escapeLike(prefix) + "%";
                keyCol.setToPreparedStatement(ps, 1, like);
            }
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String key = (String) col.getFromResultSet(rs, 1);
                    keys.add(key);
                }
            }
        }
        return keys.stream();
    }

    protected String escapeLike(String prefix) {
        return prefix.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_");
    }

    /**
     * Canonicalizes value for the database: use a String or a Long if possible.
     */
    protected Object toStorage(Object value) {
        // try to convert byte array to UTF-8 string
        if (value instanceof byte[]) {
            try {
                value = bytesToString((byte[]) value);
            } catch (CharacterCodingException e) {
                // ignore
            }
        }
        // try to convert String to Long
        if (value instanceof String) {
            try {
                value = Long.valueOf((String) value);
            } catch (NumberFormatException e) {
                // ignore
            }
        }
        return value;
    }

    protected byte[] toBytes(Object value) {
        if (value instanceof String) {
            return ((String) value).getBytes(UTF_8);
        } else if (value instanceof Long) {
            return ((Long) value).toString().getBytes(UTF_8);
        } else if (value instanceof byte[]) {
            return (byte[]) value;
        }
        return null;
    }

    protected String toString(Object value) {
        if (value instanceof String) {
            return (String) value;
        } else if (value instanceof Long) {
            return ((Long) value).toString();
        } else if (value instanceof byte[]) {
            byte[] bytes = (byte[]) value;
            try {
                return bytesToString(bytes);
            } catch (CharacterCodingException e) {
                return null;
            }
        }
        return null;
    }

    protected Long toLong(Object value) throws NumberFormatException { // NOSONAR
        if (value instanceof Long) {
            return (Long) value;
        } else if (value instanceof String) {
            return Long.valueOf((String) value);
        } else if (value instanceof byte[]) {
            byte[] bytes = (byte[]) value;
            return bytesToLong(bytes);
        }
        return null;
    }

    @Override
    public byte[] get(String key) {
        Object value = getObject(key);
        if (value == null) {
            return null;
        }
        byte[] bytes = toBytes(value);
        if (bytes != null) {
            return bytes;
        }
        throw new UnsupportedOperationException(value.getClass().getName());
    }

    @Override
    public String getString(String key) {
        Object value = getObject(key);
        if (value == null) {
            return null;
        }
        String string = toString(value);
        if (string != null) {
            return string;
        }
        throw new IllegalArgumentException("Value is not a String for key: " + key);
    }

    @Override
    public Long getLong(String key) throws NumberFormatException { // NOSONAR
        Object value = getObject(key);
        if (value == null) {
            return null;
        }
        Long longValue = toLong(value);
        if (longValue != null) {
            return longValue;
        }
        throw new NumberFormatException("Value is not a Long for key: " + key);
    }

    @Override
    public Map<String, byte[]> get(Collection<String> keys) {
        Map<String, byte[]> map = new HashMap<>(keys.size());
        getObjects(keys, (key, value) -> {
            byte[] bytes = toBytes(value);
            if (bytes == null) {
                throw new UnsupportedOperationException(String.format("Value of class %s is not supported for key: %s",
                        value.getClass().getName(), key));
            }
            map.put(key, bytes);
        });
        return map;
    }

    @Override
    public Map<String, String> getStrings(Collection<String> keys) {
        Map<String, String> map = new HashMap<>(keys.size());
        getObjects(keys, (key, value) -> {
            String string = toString(value);
            if (string == null) {
                throw new IllegalArgumentException("Value is not a String for key: " + key);
            }
            map.put(key, string);
        });
        return map;
    }

    @Override
    public Map<String, Long> getLongs(Collection<String> keys) throws NumberFormatException { // NOSONAR
        Map<String, Long> map = new HashMap<>(keys.size());
        getObjects(keys, (key, value) -> {
            Long longValue = toLong(value);
            if (longValue == null) {
                throw new IllegalArgumentException("Value is not a Long for key: " + key);
            }
            map.put(key, longValue);
        });
        return map;
    }

    protected Object getObject(String key) {
        return runWithConnection(connection -> {
            String sql = "SELECT " + longCol.getQuotedName() + ", " + stringCol.getQuotedName() + ", "
                    + bytesCol.getQuotedName() + " FROM " + table.getQuotedName() + " WHERE " + keyCol.getQuotedName()
                    + " = ?";
            log.trace("SQL: {} -- key={}", sql, key);
            try (PreparedStatement ps = connection.prepareStatement(sql)) {
                keyCol.setToPreparedStatement(ps, 1, key);
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        log.trace("  -> null");
                        return null;
                    }
                    Long longValue = (Long) longCol.getFromResultSet(rs, 1);
                    String string = (String) stringCol.getFromResultSet(rs, 2);
                    byte[] bytes = (byte[]) bytesCol.getFromResultSet(rs, 3);
                    log.trace("  -> {} {} ", string, longValue);
                    if (string != null) {
                        return string;
                    } else if (longValue != null) {
                        return longValue;
                    } else {
                        return bytes;
                    }
                }
            }
        });
    }

    protected void getObjects(Collection<String> keys, BiConsumer<String, Object> consumer) {
        if (keys.isEmpty()) {
            return;
        }
        runWithConnection((Connection connection) -> {
            StringBuilder sql = new StringBuilder();
            sql.append("SELECT ");
            sql.append(keyCol.getQuotedName());
            sql.append(", ");
            sql.append(longCol.getQuotedName());
            sql.append(", ");
            sql.append(stringCol.getQuotedName());
            sql.append(", ");
            sql.append(bytesCol.getQuotedName());
            sql.append(" FROM ");
            sql.append(table.getQuotedName());
            sql.append(" WHERE ");
            sql.append(keyCol.getQuotedName());
            sql.append(" IN (");
            for (int i = 0; i < keys.size(); i++) {
                if (i != 0) {
                    sql.append(", ");
                }
                sql.append('?');
            }
            sql.append(")");
            log.trace("SQL: {} -- keys={}", sql, keys);
            try (PreparedStatement ps = connection.prepareStatement(sql.toString())) {
                int i = 1;
                for (String key : keys) {
                    keyCol.setToPreparedStatement(ps, i++, key);
                }
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String key = (String) keyCol.getFromResultSet(rs, 1);
                        Long longVal = (Long) longCol.getFromResultSet(rs, 2);
                        String string = (String) stringCol.getFromResultSet(rs, 3);
                        byte[] bytes = (byte[]) bytesCol.getFromResultSet(rs, 4);
                        log.trace("  -> {} {} ", string, longVal);
                        Object value;
                        if (string != null) {
                            value = string;
                        } else if (longVal != null) {
                            value = longVal;
                        } else {
                            value = bytes;
                        }
                        if (value != null) {
                            consumer.accept(key, value);
                        }
                    }
                }
            }
        });
    }

    protected Long ttlToStorage(long ttl) {
        return ttl == 0 ? null : getTTLValue(ttl);
    }

    protected Long getTTLValue(long ttl) {
        return Long.valueOf(System.currentTimeMillis() / 1000 + ttl);
    }

    @Override
    public void put(String key, byte[] bytes) {
        put(key, toStorage(bytes), 0);
    }

    @Override
    public void put(String key, byte[] bytes, long ttl) {
        put(key, toStorage(bytes), ttl);
    }

    @Override
    public void put(String key, String string) {
        put(key, toStorage(string), 0);
    }

    @Override
    public void put(String key, String string, long ttl) {
        put(key, toStorage(string), ttl);
    }

    @Override
    public void put(String key, Long value) {
        put(key, (Object) value, 0);
    }

    @Override
    public void put(String key, Long value, long ttl) {
        put(key, (Object) value, ttl);
    }

    protected void put(String key, Object value, long ttl) {
        runWithConnection((Connection connection) -> {
            if (value == null) {
                // delete
                String sql = "DELETE FROM " + table.getQuotedName() + " WHERE " + keyCol.getQuotedName() + " = ?";
                log.trace("SQL: {} -- key={}", sql, key);
                try (PreparedStatement ps = connection.prepareStatement(sql)) {
                    keyCol.setToPreparedStatement(ps, 1, key);
                    ps.execute();
                }
            } else {
                // upsert (update or insert)
                Long longValue = value instanceof Long ? (Long) value : null;
                String stringValue = value instanceof String ? (String) value : null;
                byte[] bytesValue = value instanceof byte[] ? (byte[]) value : null;
                Long ttlValue = ttlToStorage(ttl);
                List<Column> outColumns = new ArrayList<>();
                List<Serializable> outValues = new ArrayList<>();
                String sql = dialect.getUpsertSql(Arrays.asList(keyCol, longCol, stringCol, bytesCol, ttlCol),
                        Arrays.asList(key, longValue, stringValue, bytesValue, ttlValue), outColumns, outValues);
                for (int retry = 0; retry < MAX_RETRY; retry++) {
                    try {
                        log.trace("SQL: {} -- key={} val={} ttl={}", sql, key, value, ttl);
                        try (PreparedStatement ps = connection.prepareStatement(sql)) {
                            for (int i = 0; i < outColumns.size(); i++) {
                                outColumns.get(i).setToPreparedStatement(ps, i + 1, outValues.get(i));
                            }
                            ps.execute();
                        }
                        return;
                    } catch (SQLException e) {
                        if (!dialect.isConcurrentUpdateException(e)) {
                            throw e;
                        }
                        // Oracle MERGE can throw DUP_VAL_ON_INDEX (ORA-0001) or NO_DATA_FOUND (ORA-01403)
                        // in that case retry a few times
                    }
                    sleepBeforeRetry();
                }
                throw new ConcurrentUpdateException("Failed to do atomic put for key: " + key);
            }
        });
    }

    @Override
    public boolean setTTL(String key, long ttl) {
        return runWithConnection((Connection connection) -> {
            String sql = "UPDATE " + table.getQuotedName() + " SET " + ttlCol.getQuotedName() + " = ? WHERE "
                    + keyCol.getQuotedName() + " = ?";
            log.trace("SQL: {} -- key={} ttl={}", sql, key, ttl);
            try (PreparedStatement ps = connection.prepareStatement(sql)) {
                ttlCol.setToPreparedStatement(ps, 1, ttlToStorage(ttl));
                keyCol.setToPreparedStatement(ps, 2, key);
                int count = ps.executeUpdate();
                boolean set = count == 1;
                return set;
            }
        }).booleanValue();
    }

    @Override
    public boolean compareAndSet(String key, byte[] expected, byte[] value, long ttl) {
        return compareAndSet(key, toStorage(expected), toStorage(value), ttl);
    }

    @Override
    public boolean compareAndSet(String key, String expected, String value, long ttl) {
        return compareAndSet(key, (Object) expected, (Object) value, ttl);
    }

    protected boolean compareAndSet(String key, Object expected, Object value, long ttl) {
        return runWithConnection((Connection connection) -> {
            if (expected == null && value == null) {
                // check that document doesn't exist
                String sql = "SELECT " + keyCol.getQuotedName() + " FROM " + table.getQuotedName() + " WHERE "
                        + keyCol.getQuotedName() + " = ?";
                log.trace("SQL: {} -- key={}", sql, key);
                try (PreparedStatement ps = connection.prepareStatement(sql)) {
                    keyCol.setToPreparedStatement(ps, 1, key);
                    try (ResultSet rs = ps.executeQuery()) {
                        boolean set = !rs.next();
                        log.trace("SQL: TEST {} = null ? {}", key, set ? "NOP" : "FAILED");
                        return set;
                    }
                }
            } else if (expected == null) {
                // set value if no document already exists: regular insert
                String sql = "INSERT INTO " + table.getQuotedName() + "(" + keyCol.getQuotedName() + ", "
                        + longCol.getQuotedName() + ", " + stringCol.getQuotedName() + ", " + bytesCol.getQuotedName()
                        + ", " + ttlCol.getQuotedName() + ") VALUES (?, ?, ?, ?, ?)";
                log.trace("SQL: {} -- key={} val={} ttl={}", sql, key, value, ttl);
                try (PreparedStatement ps = connection.prepareStatement(sql)) {
                    Long longValue = value instanceof Long ? (Long) value : null;
                    String stringValue = value instanceof String ? (String) value : null;
                    byte[] bytesValue = value instanceof byte[] ? (byte[]) value : null;
                    keyCol.setToPreparedStatement(ps, 1, key);
                    longCol.setToPreparedStatement(ps, 2, longValue);
                    stringCol.setToPreparedStatement(ps, 3, stringValue);
                    bytesCol.setToPreparedStatement(ps, 4, bytesValue);
                    ttlCol.setToPreparedStatement(ps, 5, ttlToStorage(ttl));
                    boolean set;
                    try {
                        ps.executeUpdate();
                        set = true;
                    } catch (SQLException e) {
                        if (!dialect.isConcurrentUpdateException(e)) {
                            throw e;
                        }
                        set = false;
                    }
                    return set;
                }
            } else if (value == null) {
                // delete if previous value exists
                Column col = expected instanceof Long ? longCol : expected instanceof String ? stringCol : bytesCol;
                String sql = "DELETE FROM " + table.getQuotedName() + " WHERE " + keyCol.getQuotedName() + " = ? AND "
                        + dialect.getQuotedNameForExpression(col) + " = ?";
                log.trace("SQL: {} -- key={} val={}", sql, key, expected);
                try (PreparedStatement ps = connection.prepareStatement(sql)) {
                    keyCol.setToPreparedStatement(ps, 1, key);
                    col.setToPreparedStatement(ps, 2, (Serializable) expected);
                    int count = ps.executeUpdate();
                    boolean set = count == 1;
                    log.trace("SQL: TEST {} = {} ? DEL", key, expected, set ? "DEL" : "FAILED");
                    return set;
                }
            } else {
                // replace if previous value exists
                Column expectedCol = expected instanceof Long ? longCol
                        : expected instanceof String ? stringCol : bytesCol;
                Column valueCol = value instanceof Long ? longCol : value instanceof String ? stringCol : bytesCol;
                if (expectedCol != valueCol) {
                    throw new NuxeoException("TODO expected and value have different types");
                    // TODO in that case we must set to null the old value column
                }
                String sql = "UPDATE " + table.getQuotedName() + " SET " + valueCol.getQuotedName() + " = ?, "
                        + ttlCol.getQuotedName() + " = ? WHERE " + keyCol.getQuotedName() + " = ? AND "
                        + dialect.getQuotedNameForExpression(expectedCol) + " = ?";
                try (PreparedStatement ps = connection.prepareStatement(sql)) {
                    valueCol.setToPreparedStatement(ps, 1, (Serializable) value);
                    ttlCol.setToPreparedStatement(ps, 2, ttlToStorage(ttl));
                    keyCol.setToPreparedStatement(ps, 3, key);
                    expectedCol.setToPreparedStatement(ps, 4, (Serializable) expected);
                    int count = ps.executeUpdate();
                    boolean set = count == 1;
                    if (set) {
                        log.trace("SQL: TEST {} = {} ? SET {}", key, expected, value);
                    } else {
                        log.trace("SQL: TEST {} = {} ? FAILED", key, expected);
                    }
                    return set;
                }
            }
        }).booleanValue();
    }

    @Override
    public long addAndGet(String key, long delta) throws NumberFormatException { // NOSONAR
        return runWithConnection((Connection connection) -> {
            for (int retry = 0; retry < MAX_RETRY; retry++) {
                String updateReturningSql;
                if (dialect instanceof DialectPostgreSQL) {
                    updateReturningSql = "UPDATE " + table.getQuotedName() + " SET " + longCol.getQuotedName()
                            + " = CASE WHEN " + longCol.getQuotedName() + " IS NULL THEN 1 ELSE "
                            + longCol.getQuotedName() + " + ? END WHERE " + keyCol.getQuotedName() + " = ? AND "
                            + stringCol.getQuotedName() + " IS NULL AND " + bytesCol.getQuotedName()
                            + " IS NULL RETURNING " + longCol.getQuotedName();
                } else {
                    updateReturningSql = null;
                }
                if (updateReturningSql != null) {
                    log.trace("SQL: {} -- key={} delta={}", updateReturningSql, key, delta);
                    try (PreparedStatement ps = connection.prepareStatement(updateReturningSql)) {
                        longCol.setToPreparedStatement(ps, 1, Long.valueOf(delta));
                        keyCol.setToPreparedStatement(ps, 2, key);
                        try (ResultSet rs = ps.executeQuery()) {
                            if (rs.next()) {
                                Long res = (Long) longCol.getFromResultSet(rs, 1);
                                return res.longValue();
                            }
                        }
                    }
                }
                // dialect doesn't support UPDATE RETURNING, or
                // there was no row for this key, or
                // row didn't contain a long
                // -> retry using a full transaction doing check + insert
                // start transaction
                connection.setAutoCommit(false);
                try {
                    // check value
                    String sql = "SELECT " + longCol.getQuotedName() + " FROM " + table.getQuotedName() + " WHERE "
                            + keyCol.getQuotedName() + " = ?";
                    log.trace("SQL: {} -- key={}", sql, key);
                    Long currentLong;
                    try (PreparedStatement ps = connection.prepareStatement(sql)) {
                        keyCol.setToPreparedStatement(ps, 1, key);
                        try (ResultSet rs = ps.executeQuery()) {
                            if (rs.next()) {
                                currentLong = (Long) longCol.getFromResultSet(rs, 1);
                                log.trace("  -> {}", currentLong);
                                if (currentLong == null) {
                                    throw new NumberFormatException("Value is not a Long for key: " + key);
                                }
                            } else {
                                currentLong = null;
                            }
                        }
                    }
                    if (currentLong == null) {
                        // try insert
                        String insertSql = "INSERT INTO " + table.getQuotedName() + "(" + keyCol.getQuotedName() + ", "
                                + longCol.getQuotedName() + ") VALUES (?, ?)";
                        log.trace("SQL: {} -- key={} value={}", insertSql, key, delta);
                        try (PreparedStatement ps = connection.prepareStatement(insertSql)) {
                            keyCol.setToPreparedStatement(ps, 1, key);
                            longCol.setToPreparedStatement(ps, 2, Long.valueOf(delta));
                            try {
                                ps.executeUpdate();
                                return delta;
                            } catch (SQLException e) {
                                if (!dialect.isConcurrentUpdateException(e)) {
                                    throw e;
                                }
                                // if concurrent update, retry
                            }
                        }
                    } else {
                        // update existing value
                        Long newLong = Long.valueOf(currentLong.longValue() + delta);
                        String updateSql = "UPDATE " + table.getQuotedName() + " SET " + longCol.getQuotedName()
                                + " = ? WHERE " + keyCol.getQuotedName() + " = ? AND " + longCol.getQuotedName()
                                + " = ?";
                        try (PreparedStatement ps = connection.prepareStatement(updateSql)) {
                            longCol.setToPreparedStatement(ps, 1, newLong);
                            keyCol.setToPreparedStatement(ps, 2, key);
                            longCol.setToPreparedStatement(ps, 3, currentLong);
                            int count = ps.executeUpdate();
                            if (count == 1) {
                                return newLong;
                            }
                            // else the value changed...
                            // concurrent update, retry
                        }
                    }
                } finally {
                    connection.commit();
                    connection.setAutoCommit(true);
                }
                // concurrent update on insert or update, retry a few times
                sleepBeforeRetry();
            }
            throw new ConcurrentUpdateException("Failed to do atomic addAndGet for key: " + key);
        }).longValue();
    }

    protected void sleepBeforeRetry() {
        try {
            Thread.sleep(5);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new NuxeoException(e);
        }
    }

    // works on any representation that can be converted to a Long
    protected long addAndGetGeneric(String key, long delta) throws NumberFormatException { // NOSONAR
        for (;;) {
            Object value = getObject(key);
            long result;
            if (value == null) {
                result = delta;
            } else {
                Long base = toLong(value);
                if (base == null) {
                    throw new NumberFormatException("Value is not a Long for key: " + key);
                }
                result = base.longValue() + delta;
            }
            Object newValue = Long.valueOf(result);
            if (compareAndSet(key, value, newValue, 0)) {
                return result;
            }
            // else loop to try again
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(" + name + ")";
    }

}
