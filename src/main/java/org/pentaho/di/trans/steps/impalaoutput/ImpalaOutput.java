/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.di.trans.steps.impalaoutput;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.database.DatabaseInterface;
import org.pentaho.di.core.exception.KettleDatabaseBatchException;
import org.pentaho.di.core.exception.KettleDatabaseException;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

import java.sql.Statement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * Writes rows to a database table.
 *
 * @author Matt Casters
 * @since 6-apr-2003
 */
public class ImpalaOutput extends BaseStep implements StepInterface {
  private static Class<?> PKG = ImpalaOutputMeta.class; // for i18n purposes, needed by Translator2!!

  private ImpalaOutputMeta meta;
  private ImpalaOutputData data;


  public ImpalaOutput( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
    Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {
    meta = (ImpalaOutputMeta) smi;
    data = (ImpalaOutputData) sdi;

    Object[] r = getRow(); // this also waits for a previous step to be finished.
    if ( r == null ) { // no more input to be expected...
      // truncate the table if there are no rows at all coming into this step
      if ( first && meta.truncateTable() ) {
        truncateTable();
      }
      if(!first){
          //如果是最后一行，则提交所有剩余的记录 TODO
          String p_tableName = meta.getTableName();
          Integer p_commitCounter = data.commitCounterMap.get(p_tableName);
          Statement  insertStatement = data.statements.get(p_tableName);
          try {
              String batchSql = ImpalaOutputUtils.getBatchSql(data, p_tableName);
              insertStatement.execute(batchSql);
              // System.out.println(batchSql);
              data.db.commit();
          } catch (SQLException ex) {
              throw new KettleDatabaseException("Error updating batch", ex);
          } catch (Exception ex) {
              throw new KettleDatabaseException("Unexpected error inserting row", ex);
          }
          // Clear the batch/commit counter...
          //
          data.commitCounterMap.put(p_tableName, Integer.valueOf(0));
          data.batchBuffer.clear();
          //System.out.println("最后记录提交"+p_commitCounter+p_tableName);

      }

      return false;
    }

    if ( first ) {
      first = false;
      if ( meta.truncateTable() ) {
        truncateTable();
      }
      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields( data.outputRowMeta, getStepname(), null, null, this, repository, metaStore );

      if ( !meta.specifyFields() ) {
        // Just take the input row
        data.insertRowMeta = getInputRowMeta().clone();
      } else {

        data.insertRowMeta = new RowMeta();

        //
        // Cache the position of the compare fields in Row row
        //
        data.valuenrs = new int[meta.getFieldDatabase().length];
        for ( int i = 0; i < meta.getFieldDatabase().length; i++ ) {
          data.valuenrs[i] = getInputRowMeta().indexOfValue( meta.getFieldStream()[i] );
          if ( data.valuenrs[i] < 0 ) {
            throw new KettleStepException( BaseMessages.getString(
              PKG, "ImpalaOutput.Exception.FieldRequired", meta.getFieldStream()[i] ) );
          }
        }

        for ( int i = 0; i < meta.getFieldDatabase().length; i++ ) {
          ValueMetaInterface insValue = getInputRowMeta().searchValueMeta( meta.getFieldStream()[i] );
          if ( insValue != null ) {
            ValueMetaInterface insertValue = insValue.clone();
            insertValue.setName( meta.getFieldDatabase()[i] );
            data.insertRowMeta.addValueMeta( insertValue );
          } else {
            throw new KettleStepException( BaseMessages.getString(
              PKG, "ImpalaOutput.Exception.FailedToFindField", meta.getFieldStream()[i] ) );
          }
        }
      }
    }

    try {
      Object[] outputRowData = writeToTable( getInputRowMeta(), r );

      if ( outputRowData != null ) {
        putRow( data.outputRowMeta, outputRowData ); // in case we want it go further...
        incrementLinesOutput();

      }

      if ( checkFeedback( getLinesRead() ) ) {
        if ( log.isBasic() ) {
          logBasic( "linenr " + getLinesRead() );
        }
      }
    } catch ( KettleException e ) {
      logError( "Because of an error, this step can't continue: ", e );
      setErrors( 1 );
      stopAll();
      setOutputDone(); // signal end to receiver(s)
      return false;
    }

    return true;
  }

  protected Object[] writeToTable( RowMetaInterface rowMeta, Object[] r ) throws KettleException {
        if (r == null) { // Stop: last line or error encountered
            if (log.isDetailed()) {
                logDetailed("Last line inserted: stop");
            }
            //如果是最后一行，则提交所有剩余的记录 TODO
            //String batchSql = ImpalaOutputUtils.getBatchSql(data, p_tableName);
            //System.out.println(batchSql);
            return null;
        }

        Statement insertStatement = null;
        Object[] insertRowData;
        Object[] outputRowData = r;

        String tableName = null;

        boolean sendToErrorRow = false;
        String errorMessage = null;
        boolean rowIsSafe = false;

        if (meta.isTableNameInField()) {
            // Cache the position of the table name field
            if (data.indexOfTableNameField < 0) {
                String realTablename = environmentSubstitute(meta.getTableNameField());
                data.indexOfTableNameField = rowMeta.indexOfValue(realTablename);
                if (data.indexOfTableNameField < 0) {
                    String message = "Unable to find table name field [" + realTablename + "] in input row";
                    logError(message);
                    throw new KettleStepException(message);
                }
                if (!meta.isTableNameInTable() && !meta.specifyFields()) {
                    data.insertRowMeta.removeValueMeta(data.indexOfTableNameField);
                }
            }
            tableName = rowMeta.getString(r, data.indexOfTableNameField);

            if (!meta.isTableNameInTable() && !meta.specifyFields()) {
                // If the name of the table should not be inserted itself,
                // remove the table name
                // from the input row data as well. This forcibly creates a copy
                // of r
                //
                insertRowData = RowDataUtil.removeItem(rowMeta.cloneRow(r), data.indexOfTableNameField);
            } else {
                insertRowData = r;
            }
        } else if (meta.isPartitioningEnabled() && (meta.isPartitioningDaily() || meta.isPartitioningMonthly())
                && (meta.getPartitioningField() != null && meta.getPartitioningField().length() > 0)) {
            // Initialize some stuff!
            if (data.indexOfPartitioningField < 0) {
                data.indexOfPartitioningField = rowMeta
                        .indexOfValue(environmentSubstitute(meta.getPartitioningField()));
                if (data.indexOfPartitioningField < 0) {
                    throw new KettleStepException(
                            "Unable to find field [" + meta.getPartitioningField() + "] in the input row!");
                }

                if (meta.isPartitioningDaily()) {
                    data.dateFormater = new SimpleDateFormat("yyyyMMdd");
                } else {
                    data.dateFormater = new SimpleDateFormat("yyyyMM");
                }
            }

            ValueMetaInterface partitioningValue = rowMeta.getValueMeta(data.indexOfPartitioningField);
            if (!partitioningValue.isDate() || r[data.indexOfPartitioningField] == null) {
                throw new KettleStepException(
                        "Sorry, the partitioning field needs to contain a data value and can't be empty!");
            }

            Object partitioningValueData = rowMeta.getDate(r, data.indexOfPartitioningField);
            tableName = environmentSubstitute(meta.getTableName()) + "_"
                    + data.dateFormater.format((Date) partitioningValueData);
            insertRowData = r;
        } else {
            tableName = data.tableName;
            insertRowData = r;
        }

        if (meta.specifyFields()) {
            //
            // The values to insert are those in the fields sections
            //
            insertRowData = new Object[data.valuenrs.length];
            for (int idx = 0; idx < data.valuenrs.length; idx++) {
                insertRowData[idx] = r[data.valuenrs[idx]];
            }
        }

        if (Const.isEmpty(tableName)) {
            throw new KettleStepException("The tablename is not defined (empty)");
        }

        insertStatement = data.statements.get(tableName);
        if (insertStatement == null) {
            // String sql =data.db.getInsertStatement( environmentSubstitute(
            // meta.getSchemaName() ), tableName, data.insertRowMeta );
            String sql = ImpalaOutputUtils.getInsertStatement(data.db, environmentSubstitute(meta.getSchemaName()),
                    tableName, data.insertRowMeta);
            data.sqls.put(tableName, sql);
            if (log.isDetailed()) {
                logDetailed("impala insert into table sql: " + sql);
            }
            insertStatement = ImpalaOutputUtils.createStatement(data.db);
            data.statements.put(tableName, insertStatement);

        }

        try {
            // For PG & GP, we add a savepoint before the row.
            // Then revert to the savepoint afterwards... (not a transaction, so
            // hopefully still fast)
            //
            if (data.useSafePoints) {
                data.savepoint = data.db.setSavepoint();
            }

            data.batchBuffer.add(r); // save row

            if (log.isRowLevel()) {
                logRowlevel("cache row before insert: " + data.insertRowMeta.getString(insertRowData));
            }

            // Get a commit counter per prepared statement to keep track of
            // separate tables, etc.
            Integer commitCounter = data.commitCounterMap.get(tableName);
            if (commitCounter == null) {
                commitCounter = Integer.valueOf(1);
            } else {
                commitCounter++;
            }
            data.commitCounterMap.put(tableName, Integer.valueOf(commitCounter.intValue()));

            // Release the savepoint if needed
            //
            if (data.useSafePoints) {
                if (data.releaseSavepoint) {
                    data.db.releaseSavepoint(data.savepoint);
                }
            }

            /***
             * 提交触发点取决于“提交记录数量”,"使用批量插入"的可选项将不再生效
             */
            if ((data.commitSize > 0) && ((commitCounter % data.commitSize) == 0)) {

                try {
                    String batchSql = ImpalaOutputUtils.getBatchSql(data, tableName);
                    insertStatement.execute(batchSql);
                   // System.out.println(batchSql);
                    data.db.commit();
                } catch (SQLException ex) {
                    throw new KettleDatabaseException("Error updating batch", ex);
                } catch (Exception ex) {
                    throw new KettleDatabaseException("Unexpected error inserting row", ex);
                }
                // Clear the batch/commit counter...
                //
                data.commitCounterMap.put(tableName, Integer.valueOf(0));
                rowIsSafe = true;
            } else {
                rowIsSafe = false;
            }

        } catch (KettleDatabaseException dbe) {
            if (getStepMeta().isDoingErrorHandling()) {
                if (log.isRowLevel()) {
                    logRowlevel("Written row to error handling : " + getInputRowMeta().getString(r));
                }

                if (data.useSafePoints) {
                    data.db.rollback(data.savepoint);
                    if (data.releaseSavepoint) {
                        data.db.releaseSavepoint(data.savepoint);
                    }
                    // data.db.commit(true); // force a commit on the connection
                    // too.
                }

                sendToErrorRow = true;
                errorMessage = dbe.toString();
            } else {
                if (meta.ignoreErrors()) {
                    if (data.warnings < 20) {
                        if (log.isBasic()) {
                            logBasic("WARNING: Couldn't insert row into table: " + rowMeta.getString(r) + Const.CR
                                    + dbe.getMessage());
                        }
                    } else if (data.warnings == 20) {
                        if (log.isBasic()) {
                            logBasic("FINAL WARNING (no more then 20 displayed): Couldn't insert row into table: "
                                    + rowMeta.getString(r) + Const.CR + dbe.getMessage());
                        }
                    }
                    data.warnings++;
                } else {
                    setErrors(getErrors() + 1);
                    data.db.rollback();
                    throw new KettleException(
                            "Error inserting row into table [" + tableName + "] with values: " + rowMeta.getString(r),
                            dbe);
                }
            }
        }

        if (sendToErrorRow) {
            // Simply add this row to the error row
            putError(rowMeta, r, data.commitSize, errorMessage, null, "TOP001");
            outputRowData = null;
        } else {
            outputRowData = null;

            // A commit was done and the rows are all safe (no error)
            if (rowIsSafe) {
                for (int i = 0; i < data.batchBuffer.size(); i++) {
                    Object[] row = data.batchBuffer.get(i);
                    putRow(data.outputRowMeta, row);
                    incrementLinesOutput();
                }
                // Clear the buffer
                data.batchBuffer.clear(); //提交后，清空缓存
            }
        }


        return outputRowData;
  }



  public boolean isRowLevel() {
    return log.isRowLevel();
  }

  private void processBatchException( String errorMessage, int[] updateCounts, List<Exception> exceptionsList ) throws KettleException {
    // There was an error with the commit
    // We should put all the failing rows out there...
    //
    if ( updateCounts != null ) {
      int errNr = 0;
      for ( int i = 0; i < updateCounts.length; i++ ) {
        Object[] row = data.batchBuffer.get( i );
        if ( updateCounts[i] > 0 ) {
          // send the error foward
          putRow( data.outputRowMeta, row );
          incrementLinesOutput();
        } else {
          String exMessage = errorMessage;
          if ( errNr < exceptionsList.size() ) {
            SQLException se = (SQLException) exceptionsList.get( errNr );
            errNr++;
            exMessage = se.toString();
          }
          putError( data.outputRowMeta, row, 1L, exMessage, null, "TOP0002" );
        }
      }
    } else {
      // If we don't have update counts, it probably means the DB doesn't support it.
      // In this case we don't have a choice but to consider all inserted rows to be error rows.
      //
      for ( int i = 0; i < data.batchBuffer.size(); i++ ) {
        Object[] row = data.batchBuffer.get( i );
        putError( data.outputRowMeta, row, 1L, errorMessage, null, "TOP0003" );
      }
    }

    // Clear the buffer afterwards...
    data.batchBuffer.clear();
  }

  public boolean init( StepMetaInterface smi, StepDataInterface sdi ) {
    meta = (ImpalaOutputMeta) smi;
    data = (ImpalaOutputData) sdi;

    if ( super.init( smi, sdi ) ) {
      try {
        data.commitSize = Integer.parseInt( environmentSubstitute( meta.getCommitSize() ) );

        data.databaseMeta = meta.getDatabaseMeta();
        DatabaseInterface dbInterface = data.databaseMeta.getDatabaseInterface();

        // Batch updates are not supported on PostgreSQL (and look-a-likes)
        // together with error handling (PDI-366).
        // For these situations we can use savepoints to help out.
        //
        data.useSafePoints =
          data.databaseMeta.getDatabaseInterface().useSafePoints() && getStepMeta().isDoingErrorHandling();

        // Get the boolean that indicates whether or not we can/should release
        // savepoints during data load.
        //
        data.releaseSavepoint = dbInterface.releaseSavepoint();

        // Disable batch mode in case
        // - we use an unlimited commit size
        // - if we need to pick up auto-generated keys
        // - if you are running the transformation as a single database transaction (unique connections)
        // - if we are reverting to save-points
        //
        data.batchMode =
          meta.useBatchUpdate()
            && data.commitSize > 0 && !meta.isReturningGeneratedKeys()
            && !getTransMeta().isUsingUniqueConnections() && !data.useSafePoints;

        // Per PDI-6211 : give a warning that batch mode operation in combination with step error handling can lead to
        // incorrectly processed rows.
        //
        if ( getStepMeta().isDoingErrorHandling() && !dbInterface.supportsErrorHandlingOnBatchUpdates() ) {
          log.logMinimal( BaseMessages.getString(
            PKG, "ImpalaOutput.Warning.ErrorHandlingIsNotFullySupportedWithBatchProcessing" ) );
        }

        if ( meta.getDatabaseMeta() == null ) {
          throw new KettleException( BaseMessages.getString(
            PKG, "ImpalaOutput.Exception.DatabaseNeedsToBeSelected" ) );
        }
        if ( meta.getDatabaseMeta() == null ) {
          logError( BaseMessages.getString( PKG, "ImpalaOutput.Init.ConnectionMissing", getStepname() ) );
          return false;
        }

        if ( !dbInterface.supportsStandardTableOutput() ) {
          throw new KettleException( dbInterface.getUnsupportedTableOutputMessage() );
        }

        data.db = new Database( this, meta.getDatabaseMeta() );
        data.db.shareVariablesWith( this );

        if ( getTransMeta().isUsingUniqueConnections() ) {
          synchronized ( getTrans() ) {
            data.db.connect( getTrans().getTransactionId(), getPartitionID() );
          }
        } else {
          data.db.connect( getPartitionID() );
        }

        if ( log.isBasic() ) {
          logBasic( "Connected to database [" + meta.getDatabaseMeta() + "] (commit=" + data.commitSize + ")" );
        }

        // Postpone commit as long as possible. PDI-2091
        //
        if ( data.commitSize == 0 ) {
          data.commitSize = Integer.MAX_VALUE;
        }
        data.db.setCommit( data.commitSize );

        if ( !meta.isPartitioningEnabled() && !meta.isTableNameInField() ) {
          data.tableName = environmentSubstitute( meta.getTableName() );
        }

        return true;
      } catch ( KettleException e ) {
        logError( "An error occurred intialising this step: " + e.getMessage() );
        stopAll();
        setErrors( 1 );
      }
    }
    return false;
  }

  void truncateTable() throws KettleDatabaseException {
    if ( !meta.isPartitioningEnabled() && !meta.isTableNameInField() ) {
      // Only the first one truncates in a non-partitioned step copy
      //
      if ( meta.truncateTable()
        && ( ( getCopy() == 0 && getUniqueStepNrAcrossSlaves() == 0 ) || !Utils.isEmpty( getPartitionID() ) ) ) {
        data.db.truncateTable( environmentSubstitute( meta.getSchemaName() ), environmentSubstitute( meta
          .getTableName() ) );

      }
    }
  }

  public void dispose( StepMetaInterface smi, StepDataInterface sdi ) {
      meta = (ImpalaOutputMeta) smi;
      data = (ImpalaOutputData) sdi;

      if ( data.db != null ) {
          try {
              for ( String schemaTable : data.statements.keySet() ) {
                  // Get a commit counter per prepared statement to keep track of separate tables, etc.
                  //
                  Integer batchCounter = data.commitCounterMap.get( schemaTable );
                  if ( batchCounter == null ) {
                      batchCounter = 0;
                  }

                  Statement insertStatement = data.statements.get( schemaTable );


                  //data.db.emptyAndCommit( insertStatement, data.batchMode, batchCounter );
              }
              for ( int i = 0; i < data.batchBuffer.size(); i++ ) {
                  Object[] row = data.batchBuffer.get( i );
                  putRow( data.outputRowMeta, row );
                  incrementLinesOutput();
              }

              // Clear the buffer
              data.batchBuffer.clear();
          } catch ( Exception dbe ) {
              logError( "Unexpected error committing the database connection.", dbe );
              logError( Const.getStackTracker( dbe ) );
              setErrors( 1 );
              stopAll();
          } finally {
              setOutputDone();

              if ( getErrors() > 0 ) {
                  try {
                      data.db.rollback();
                  } catch ( KettleDatabaseException e ) {
                      logError( "Unexpected error rolling back the database connection.", e );
                  }
              }

              data.db.disconnect();
          }
          super.dispose( smi, sdi );
      }

  }

  /**
   * Allows subclasses of TableOuput to get hold of the step meta
   *
   * @return
   */
  protected ImpalaOutputMeta getMeta() {
    return meta;
  }

  /**
   * Allows subclasses of TableOutput to get hold of the data object
   *
   * @return
   */
  protected ImpalaOutputData getData() {
    return data;
  }

  protected void setMeta( ImpalaOutputMeta meta ) {
    this.meta = meta;
  }

  protected void setData( ImpalaOutputData data ) {
    this.data = data;
  }
}
