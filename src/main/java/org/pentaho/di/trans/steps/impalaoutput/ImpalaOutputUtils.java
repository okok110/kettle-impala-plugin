package org.pentaho.di.trans.steps.impalaoutput;

import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.exception.KettleDatabaseException;

import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.sql.*;

public class ImpalaOutputUtils {

    /**
     * 获取insert into table values语句
     * @param db
     * @param schemaName
     * @param tableName
     * @param fields
     * @return
     */
    public static String getInsertStatement(Database db, String schemaName, String tableName, RowMetaInterface fields) {
        StringBuffer ins = new StringBuffer(128);

        String schemaTable = db.getDatabaseMeta().getQuotedSchemaTableCombination(schemaName, tableName);
        ins.append("INSERT INTO ").append(schemaTable).append(" (");

        // now add the names in the row:
        for (int i = 0; i < fields.size(); i++) {
            if (i > 0) {
                ins.append(", ");
            }
            String name = fields.getValueMeta(i).getName();
            ins.append(db.getDatabaseMeta().quoteField(name));
        }
        ins.append(") VALUES ");

        return ins.toString();
    }

    public static Statement createStatement(Database db) throws KettleDatabaseException {
        try {
            return db.getConnection().createStatement();
        } catch (SQLException e) {
            throw new KettleDatabaseException("Couldn't create statement:", e);
        }
    }

    /**
     * 构建 (value1,value2,value3),(value1,value2,value3),(value1,value2,value3)
     * @param data
     * @param tableName
     * @return
     * @throws KettleDatabaseException
     */
    public static String getBatchSql(ImpalaOutputData data, String tableName) throws KettleDatabaseException {
        StringBuffer sql = new StringBuffer(data.sqls.get(tableName));

        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String p_temp = "";
        for (Object[] row : data.batchBuffer) {
            StringBuffer rowValues = new StringBuffer();
            for (int i = 0; i < data.insertRowMeta.size(); i++) {
                ValueMetaInterface v = data.insertRowMeta.getValueMeta(i);
                Object cell = row[i];
                try {
                    //System.out.println(v.getType()+"&&&&&&&");
                    switch (v.getType()) {
                    case ValueMetaInterface.TYPE_NUMBER:
                        rowValues.append(v.getNumber(cell).doubleValue()).append(",");
                        break;
                    case ValueMetaInterface.TYPE_INTEGER:
                        rowValues.append(v.getInteger(cell).intValue()).append(",");
                        break;
                    case ValueMetaInterface.TYPE_STRING:
                        rowValues.append("\"").append(v.getString(cell)).append("\"").append(",");
                        break;
                    case ValueMetaInterface.TYPE_BOOLEAN:
                        rowValues.append(v.getBoolean(cell).booleanValue()).append(",");
                        break;
                    case ValueMetaInterface.TYPE_BIGNUMBER:
                        rowValues.append(v.getBigNumber(cell)).append(",");
                        break;
                    case ValueMetaInterface.TYPE_TIMESTAMP:
                        p_temp =formatter.format(Timestamp.valueOf(v.getString(cell).replace('/','-')));


                        rowValues.append("\"").append(p_temp).append("\"").append(",");
                        break;


                    default:

                        rowValues.append("\"").append(v.getString(cell)).append("\"").append(",");
                        break;
                    }
                } catch (Exception e) {
                    throw new KettleDatabaseException(
                            "Error setting value #" + i + " [" + v.toStringMeta() + "] on prepared statement", e);
                }

            }
            rowValues.setCharAt(rowValues.length() - 1, ' ');
            sql.append("(").append(rowValues).append("),");
        }
        sql.setCharAt(sql.length()-1, ' ');
        return sql.toString();
    }
}
