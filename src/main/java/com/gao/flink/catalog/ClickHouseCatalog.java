package com.gao.flink.catalog;

import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.connector.jdbc.catalog.AbstractJdbcCatalog;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.constraints.UniqueConstraint;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotEmptyException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.factories.FunctionDefinitionFactory;
import org.apache.flink.table.factories.TableFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import ru.yandex.clickhouse.domain.ClickHouseDataType;

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

    /**
     * 1、创造执行环境
     */
    public static StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
    public static EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
    public static StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(streamExecutionEnvironment, environmentSettings);


    public static void main(String[] args) throws DatabaseNotExistException, TableNotExistException {
        ClickHouseCatalog clickHouseCatalog = getClickHouseCatalog();
        runBasicApiFromCKCatalog(clickHouseCatalog);

//        getDatasFromCKCatalog();

    }

    private static void getDatasFromCKCatalog() {
        ClickHouseCatalog clickHouseCatalog = getClickHouseCatalog();
        streamTableEnvironment.registerCatalog(clickHouseCatalog.getName(), clickHouseCatalog);
        //streamTableEnvironment.useCatalog(clickHouseCatalog.getName());

        Table table = streamTableEnvironment.sqlQuery("select * from myCk.roman.t_order_mt");
        ArrayList<Row> rows = Lists.newArrayList(table.execute().collect());
        rows.forEach(System.out::println);
    }

    private static void runBasicApiFromCKCatalog(ClickHouseCatalog clickHouseCatalog) throws DatabaseNotExistException, TableNotExistException {
        //列出所有的数据库
        List<String> strings = clickHouseCatalog.listDatabases();
        //查看数据库是否存在
        boolean roman = clickHouseCatalog.databaseExists("roman");
        //获取数据库
        CatalogDatabase roman1 = clickHouseCatalog.getDatabase("roman");
        //展示某一个数据库下所有的表
        List<String> roman2 = clickHouseCatalog.listTables("roman");
        //获取某表下的schema信息。
        CatalogBaseTable roman3 = clickHouseCatalog.getTable(new ObjectPath("roman", "t_order_mt"));
        TableSchema schema = roman3.getSchema();
        String[] fieldNames = schema.getFieldNames();
        int length = fieldNames.length;
        for (int i = 0; length > i; i++) {
            Optional<DataType> fieldDataType = schema.getFieldDataType(fieldNames[i]);
            System.out.println("tableFiled :" + fieldNames[i] + " fieldDataType is " + fieldDataType.get().getLogicalType().getTypeRoot());
        }
    }

    private static ClickHouseCatalog getClickHouseCatalog() {
        ClickHouseCatalog clickHouseCatalog = new ClickHouseCatalog("myCk",
                "roman",
                "default",
                "Rootmaster@777"
                , "jdbc:clickhouse://10.0.6.84:18100/");
        return clickHouseCatalog;
    }


    /**
     * 符合父类的构造器
     *
     * @param catalogName
     * @param defaultDatabase
     * @param username
     * @param pwd
     * @param baseUrl
     */
    public ClickHouseCatalog(String catalogName, String defaultDatabase, String username, String pwd, String baseUrl) {
        super(catalogName, defaultDatabase, username, pwd, baseUrl);
    }

    @Override
    public Optional<TableFactory> getTableFactory() {
        return super.getTableFactory();
    }

    @Override
    public Optional<FunctionDefinitionFactory> getFunctionDefinitionFactory() {
        return super.getFunctionDefinitionFactory();
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        List<String> ckDatabases = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection(defaultUrl, username, pwd)) {
            PreparedStatement ps = conn.prepareStatement("SELECT * FROM system.databases;");
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                ckDatabases.add(rs.getString(1));
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
            //todo:返回的是：database的属性值对+注释（null）
            return new CatalogDatabaseImpl(Collections.emptyMap(), null);
        } else {
            throw new DatabaseNotExistException(getName(), databaseName);
        }
    }

    @Override
    public void dropDatabase(String name, boolean ignoreIfNotExists) throws DatabaseNotExistException, DatabaseNotEmptyException, CatalogException {
        super.dropDatabase(name, ignoreIfNotExists);
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
        //todo：通过指定数据库获取表列表？？？
        //baseUrl + databaseName
        try (Connection conn = DriverManager.getConnection(baseUrl, username, pwd)) {
            PreparedStatement ps = conn.prepareStatement("SELECT * FROM system.tables where database = ?;");
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
        try (Connection conn = DriverManager.getConnection(dbUrl, username, pwd)) {
            PreparedStatement ps =
                    conn.prepareStatement(String.format("SELECT * FROM %s;", tablePath.getFullName()));
            //set table Name and type(change to flink field type)
            ResultSetMetaData rsmd = ps.getMetaData();
            String[] names = new String[rsmd.getColumnCount()];
            DataType[] types = new DataType[rsmd.getColumnCount()];
            for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                names[i - 1] = rsmd.getColumnName(i);
                //todo:类型转换
                types[i - 1] = fromJDBCType(rsmd, i);
                if (rsmd.isNullable(i) == ResultSetMetaData.columnNoNulls) {
                    types[i - 1] = types[i - 1].notNull();
                }
            }
            TableSchema.Builder tableBuilder = new TableSchema.Builder().fields(names, types);

            //set pk
            //todo:1、ck的实现返回为空
            //DatabaseMetaData metaData = conn.getMetaData();
            //UniqueConstraint.primaryKey()

            //Connection conn1 = DriverManager.getConnection(dbUrl, username, pwd);
            PreparedStatement ps1 =
                    conn.prepareStatement("SELECT primary_key FROM system.tables where name = ?;");
            ps1.setString(1, tablePath.getObjectName());
            ResultSet rs = ps1.executeQuery();

            Optional<UniqueConstraint> primaryKey;
            String[] pknames = new String[0];
            String pkName = "";
            while (rs.next()) {
                pkName = rs.getString(1);
                pknames = pkName.split(",");
            }

            List<String> pkFields = Arrays.asList(pknames); // initialize size
            if (CollectionUtils.isNotEmpty(pkFields)) {
                primaryKey = Optional.of(UniqueConstraint.primaryKey(pkName, pkFields));
            } else {
                primaryKey = Optional.empty();
            }
            System.out.println("pkFields" + pkFields);

//            Optional<UniqueConstraint> primaryKey =
//                    getPrimaryKey(metaData, null, tablePath.getObjectName());
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
    //todo：有些类型没有转换完全
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
    public static final String NOTHING = "Nothing";
    public static final String NESTED = "Nested";
    public static final String TUPLE = "Tuple";
    public static final String ARRAY = "Array";
    public static final String AGGREGATEFUNCTION = "AggregateFunction";
    public static final String UNKNOWN = "Unknown";

    /**
     * Converts clickhouse type to Flink {@link DataType}.
     * todo:
     * 拉取数据时，flink的类型要>=ck的数据类型
     * sink数据时，flink的类型要<=ck的数据类型？
     * 所以类型要对应统一？
     *
     * @see ClickHouseDataType
     */
    private DataType fromJDBCType(ResultSetMetaData metadata, int colIndex) throws SQLException {
        String pgType = metadata.getColumnTypeName(colIndex);
        if (pgType.contains("(")) {
            pgType = pgType.substring(0, pgType.indexOf("("));
        }

        int precision = metadata.getPrecision(colIndex);
        int scale = metadata.getScale(colIndex);

        switch (pgType) {
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
            //todo:
//            case UUID:
//                return DataTypes.STRING();
            case FIXEDSTRING:
            case STRING:
                return DataTypes.STRING();
            //todo:flink的Array需要指定array的类型
//            case ARRAY:
//                return DataTypes.ARRAY();
            default:
                throw new UnsupportedOperationException(
                        String.format("Doesn't support Postgres type '%s' yet", pgType));
        }
    }

}
