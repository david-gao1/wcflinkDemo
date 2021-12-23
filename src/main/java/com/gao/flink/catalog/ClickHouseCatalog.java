package com.gao.flink.catalog;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.connector.jdbc.catalog.AbstractJdbcCatalog;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.constraints.UniqueConstraint;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.types.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.clickhouse.BalancedClickhouseDataSource;
import ru.yandex.clickhouse.domain.ClickHouseDataType;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.sql.*;
import java.util.*;

import static org.apache.flink.connector.jdbc.table.JdbcDynamicTableFactory.*;
import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;

/**
 * @Description TODO
 * @Author roman.gao
 * @Date 2021/8/29 3:31 下午
 */
public class ClickHouseCatalog extends AbstractJdbcCatalog {

    public static Logger log = LoggerFactory.getLogger(ClickHouseCatalog.class);
    private static final Set<String> builtinDatabases =
            new HashSet<String>(Arrays.asList("system","_cw_distributed_db","_temporary_and_external_tables"));

    private static ClickHouseCatalog clickHouseCatalog;
    private static String ckCatalogName;
    private static String ckDefaultDatabase;
    private static String ckUsername;
    private static String ckPwd;
    private static String ckBaseUrl;

    static {
        try {
            Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
        } catch (ClassNotFoundException e) {
            log.error("load ClickHouseDriver result ClassNotFoundException");
        }
    }

    private ClickHouseCatalog(String catalogName, String defaultDatabase, String username, String pwd, String baseUrl) {
        super(catalogName, defaultDatabase, username, pwd, baseUrl);
        ckCatalogName = catalogName;
        ckDefaultDatabase = defaultDatabase;
        ckUsername = username;
        ckPwd = pwd;
        ckBaseUrl = baseUrl;
    }

    public static synchronized ClickHouseCatalog getInstance(String catalogName,
                                                             String defaultDatabase,
                                                             String username, String pwd, String baseUrl) {
        if (clickHouseCatalog == null || !(catalogName.equals(ckCatalogName) && defaultDatabase.equals(ckDefaultDatabase)
                && username.equals(ckUsername) && pwd.equals(ckPwd) && baseUrl.equals(ckBaseUrl))) {
            clickHouseCatalog = new ClickHouseCatalog(catalogName, defaultDatabase, username, pwd, baseUrl);
        }
        return clickHouseCatalog;
    }

    public static Boolean hasCatalogInstance() {
        return clickHouseCatalog != null;
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        List<String> ckDatabases = new ArrayList<>();
        //支持clickhouse集群
        ClickHouseProperties properties = new ClickHouseProperties();
        properties.setUser(StringUtils.isBlank(username) ? null : username);
        properties.setPassword(StringUtils.isBlank(pwd) ? null : pwd);
        BalancedClickhouseDataSource dataSource;
        dataSource = new BalancedClickhouseDataSource(defaultUrl, properties);
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement("SELECT * FROM system.databases;")) {
            ResultSet rs = ps.executeQuery();
            //过滤默认的数据库
            while (rs.next()) {
                String databaseName = rs.getString(1);
                if (!builtinDatabases.contains(databaseName)) {
                    ckDatabases.add(databaseName);
                }
            }
            return ckDatabases;
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed listing database in catalog %s", getName()), e);
        }
    }

    @Override
    public CatalogDatabase getDatabase(String databaseName) throws DatabaseNotExistException, CatalogException {
        if (listDatabases().contains(databaseName)) {
            //返回的是：database的属性值对+注释（null）
            return new CatalogDatabaseImpl(Collections.emptyMap(), null);
        } else {
            throw new DatabaseNotExistException(getName(), databaseName);
        }
    }

    /**
     * 获取表名：databaseName.tableName
     *
     * @param databaseName
     * @return
     * @throws DatabaseNotExistException
     * @throws CatalogException
     */
    @Override
    public List<String> listTables(String databaseName) throws DatabaseNotExistException, CatalogException {
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(getName(), databaseName);
        }
        //baseUrl + databaseName
        //支持clickhouse集群
        ClickHouseProperties properties = new ClickHouseProperties();
        properties.setUser(StringUtils.isBlank(username) ? null : username);
        properties.setPassword(StringUtils.isBlank(pwd) ? null : pwd);
        BalancedClickhouseDataSource dataSource = new BalancedClickhouseDataSource(defaultUrl, properties);
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement("SELECT * FROM system.tables where database = ?;")
        ) {
            ps.setString(1, databaseName);
            ResultSet rs = ps.executeQuery();

            List<String> tables = new ArrayList<>();
            while (rs.next()) {
                String dataBaseName = rs.getString(1);
                String tableName = rs.getString(2);
                tables.add(tableName);
            }
            return tables;
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed listing database in catalog %s", getName()), e);
        }
    }

    /**
     * 通过databaseName + tableName获取 CatalogBaseTable
     *
     * @param tablePath
     * @return
     * @throws TableNotExistException
     * @throws CatalogException
     */
    @Override
    public CatalogBaseTable getTable(ObjectPath tablePath) throws TableNotExistException, CatalogException {
        if (!tableExists(tablePath)) {
            throw new TableNotExistException(getName(), tablePath);
        }
        String dbUrl = baseUrl + tablePath.getDatabaseName();

        //支持clickhouse集群
        ClickHouseProperties properties = new ClickHouseProperties();
        properties.setUser(StringUtils.isBlank(username) ? null : username);
        properties.setPassword(StringUtils.isBlank(pwd) ? null : pwd);
        BalancedClickhouseDataSource dataSource = new BalancedClickhouseDataSource(defaultUrl, properties);

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps =
                     conn.prepareStatement(String.format("SELECT * FROM %s;", tablePath.getFullName()));
             PreparedStatement ps1 =
                     conn.prepareStatement("SELECT primary_key FROM system.tables where database = ? and name = ?;")
        ) {
            //set table Name and type(change to flink field type)
            ResultSetMetaData rsmd = ps.getMetaData();
            String[] names = new String[rsmd.getColumnCount()];
            DataType[] types = new DataType[rsmd.getColumnCount()];
            for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                names[i - 1] = rsmd.getColumnName(i);
                types[i - 1] = fromJDBCType(rsmd, i);
                if (rsmd.isNullable(i) == ResultSetMetaData.columnNoNulls) {
                    types[i - 1] = types[i - 1].notNull();
                }
            }
            TableSchema.Builder tableBuilder = new TableSchema.Builder().fields(names, types);

            //set pk
            ps1.setString(1, tablePath.getDatabaseName());
            ps1.setString(2, tablePath.getObjectName());
            ResultSet rs = ps1.executeQuery();

            Optional<UniqueConstraint> primaryKey;
            String[] pknames = new String[0];
            String pkName = "";
            while (rs.next()) {
                pkName = rs.getString(1);
                if (StringUtils.isNotEmpty(pkName)) {
                    pknames = pkName.split(",");
                }
            }
            ArrayList<String> pkFieldsName = new ArrayList<>();
            List<String> tableFiledNames = Arrays.asList(names);
            for (String pkname : pknames) {
                if (tableFiledNames.contains(pkname)) {
                    pkFieldsName.add(pkname);
                }
            }
//            List<String> pkFields = Arrays.asList(pknames); // initialize size
            if (CollectionUtils.isNotEmpty(pkFieldsName)) {
                primaryKey = Optional.of(UniqueConstraint.primaryKey(pkName, pkFieldsName));
            } else {
                primaryKey = Optional.empty();
            }
            //log.warn("[getTable] get pkFields {}", pkFields);
            primaryKey.ifPresent(
                    pk ->
                            tableBuilder.primaryKey(
                                    pk.getName(), pk.getColumns().toArray(new String[0])));
            TableSchema tableSchema = tableBuilder.build();

            Map<String, String> props = new HashMap<>();
            props.put(CONNECTOR.key(), IDENTIFIER);
            props.put(URL.key(), dbUrl);
            props.put(TABLE_NAME.key(), tablePath.getObjectName());
            props.put(USERNAME.key(), username);
            props.put(PASSWORD.key(), pwd);

            return new CatalogTableImpl(tableSchema, props, "");
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed getting table %s", tablePath.getFullName()), e);
        }
    }

    @Override
    public boolean tableExists(ObjectPath tablePath) throws CatalogException {
        List<String> tables = null;
        try {
            tables = listTables(tablePath.getDatabaseName());
        } catch (DatabaseNotExistException e) {
            return false;
        }
        return tables.contains(tablePath.getObjectName());
    }


    // clickhouse type:
    //todo：有些类型暂未支持
    public static final String INTERVALYEAR = "IntervalYear";
    public static final String INTERVALQUARTER = "IntervalQuarter";
    public static final String INTERVALMONTH = "IntervalMonth";
    public static final String INTERVALWEEK = "IntervalWeek";
    public static final String INTERVALDAY = "IntervalDay";
    public static final String INTERVALHOUR = "IntervalHour";
    public static final String INTERVALMINUTE = "IntervalMinute";
    public static final String INTERVALSECOND = "IntervalSecond";
    public static final String UINT64 = "UInt64";
    public static final String UINT32 = "UInt32";
    public static final String UINT16 = "UInt16";
    public static final String UINT8 = "UInt8";
    public static final String INT64 = "Int64";
    public static final String INT32 = "Int32";
    public static final String INT16 = "Int16";
    public static final String INT8 = "Int8";
    public static final String DATE = "Date";
    public static final String DATETIME = "DateTime";
    public static final String ENUM8 = "Enum8";
    public static final String ENUM16 = "Enum16";
    public static final String FLOAT32 = "Float32";
    public static final String FLOAT64 = "Float64";
    public static final String DECIMAL32 = "Decimal32";
    public static final String DECIMAL64 = "Decimal64";
    public static final String DECIMAL128 = "Decimal128";
    public static final String DECIMAL = "Decimal";
    public static final String UUID = "UUID";
    public static final String STRING = "String";
    public static final String FIXEDSTRING = "FixedString";
    //不进行转换的类型
    public static final String NOTHING = "Nothing";
    public static final String NESTED = "Nested";
    public static final String TUPLE = "Tuple";
    public static final String ARRAY = "Array";
    public static final String AGGREGATEFUNCTION = "AggregateFunction";
    public static final String UNKNOWN = "Unknown";

    /**
     * Converts clickhouse type to Flink {@link DataType}.
     *
     * @see ClickHouseDataType
     */
    private DataType fromJDBCType(ResultSetMetaData metadata, int colIndex) throws SQLException {
        String ckType = metadata.getColumnTypeName(colIndex);
        if (ckType.contains("(")) {
            ckType = ckType.substring(0, ckType.indexOf("("));
        }

        int precision = metadata.getPrecision(colIndex);
        int scale = metadata.getScale(colIndex);

        switch (ckType) {
            case INTERVALYEAR:
            case INTERVALQUARTER:
            case INTERVALMONTH:
            case INTERVALWEEK:
            case INTERVALDAY:
            case INTERVALHOUR:
            case INTERVALMINUTE:
            case INTERVALSECOND:
            case INT32:
                return DataTypes.INT();
            case UINT32:
            case INT64:
            case UINT64:
                return DataTypes.BIGINT();
            case UINT16:
            case INT16:
                return DataTypes.SMALLINT();
            case UINT8:
            case INT8:
                return DataTypes.TINYINT();
            case DATE:
                return DataTypes.DATE();
            case DATETIME:
                return DataTypes.TIMESTAMP();
            case ENUM8:
            case ENUM16:
            case FIXEDSTRING:
            case STRING:
                return DataTypes.STRING();
            case FLOAT32:
                return DataTypes.FLOAT();
            case FLOAT64:
                return DataTypes.DOUBLE();
            case DECIMAL32:
                return DataTypes.DECIMAL(9, 9);
            case DECIMAL64:
                return DataTypes.DECIMAL(18, 18);
            case DECIMAL128:
                return DataTypes.DECIMAL(38, 38);
            case DECIMAL:
                return DataTypes.DECIMAL(precision, scale);
            case UUID:
                return DataTypes.STRING();
            default:
                return DataTypes.STRING();
//                throw new UnsupportedOperationException(
//                        String.format("Doesn't support Postgres type '%s' yet", pgType));
//
        }
    }

}
