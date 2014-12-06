/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2014 Markus Brigl
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package info.brigl.mdb;

import java.io.File;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.tmatesoft.sqljet.core.SqlJetException;
import org.tmatesoft.sqljet.core.SqlJetTransactionMode;
import org.tmatesoft.sqljet.core.table.ISqlJetTable;
import org.tmatesoft.sqljet.core.table.SqlJetDb;

import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.DatabaseBuilder;
import com.healthmarketscience.jackcess.Index;
import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.impl.IndexData.ColumnDescriptor;

/**
 * Exports a MS Access Database to SQLiteF
 */
public class MDB2SQLite {

	/**
	 * Export the Access database to the given SQLite database. The referenced
	 * SQLite database should be empty.
	 * 
	 * @param mdbFile The MS Access file.
	 * @param sqliteFile The SQLite file.
	 * @throws SQLException
	 * @throws SqlJetException
	 */
	public static void export(File mdbFile, File sqliteFile) throws Exception {
		Database mdb = DatabaseBuilder.open(mdbFile);

		SqlJetDb sqlite = SqlJetDb.open(sqliteFile, true);
		sqlite.getOptions().setAutovacuum(true);
		sqlite.beginTransaction(SqlJetTransactionMode.WRITE);

		// Create the tables
		MDB2SQLite.createTables(mdb, sqlite);

		// Populate the tables
		for (String tableName : mdb.getTableNames()) {
			MDB2SQLite.populateTable(sqlite, mdb.getTable(tableName));
		}
	}

	/**
	 * Iterate over the MDB database and create SQLite tables for every table
	 * defined in the MS Access database.
	 * 
	 * @param jdbc The SQLite database JDBC connection
	 */
	@SuppressWarnings("unchecked")
	private static void createTables(Database mdb, SqlJetDb sqlite) throws Exception {
		for (String tableName : mdb.getTableNames()) {
			Table table = mdb.getTable(tableName);
			sqlite.beginTransaction(SqlJetTransactionMode.WRITE);
			try {
				sqlite.createTable(MDB2SQLite.createTableStatement(table));
				for (Index index : (List<Index>) table.getIndexes()) {
					sqlite.createIndex(MDB2SQLite.createIndexStatement(index));
				}
			} finally {
				sqlite.commit();
			}
		}
	}

	/**
	 * Create an SQLite table for the corresponding MS Access table.
	 * 
	 * @param table MS Access table
	 * @throws SQLException
	 */
	@SuppressWarnings("unchecked")
	private static String createTableStatement(Table table) throws SQLException {
		List<String> columns = new ArrayList<String>();
		for (Column column : (List<Column>) table.getColumns()) {
			switch (column.getType()) {
			// Blob
				case BINARY:
				case OLE:
					columns.add(String.format("%s BLOB", MDB2SQLite.escape(column.getName())));
					break;

				// Integers
				case BOOLEAN:
				case BYTE:
				case INT:
				case LONG:
					columns.add(String.format("%s INTEGER", MDB2SQLite.escape(column.getName())));
					break;

				// Floating point
				case DOUBLE:
				case FLOAT:
				case NUMERIC:
					columns.add(String.format("%s REAL", MDB2SQLite.escape(column.getName())));
					break;

				// TEXT
				case TEXT:
				case GUID:
				case MEMO:
				case MONEY:
				case SHORT_DATE_TIME:
					columns.add(String.format("%s TEXT", MDB2SQLite.escape(column.getName())));
					break;

				default:
					throw new SQLException("Unhandled MS Acess datatype: " + column.getType());
			}
		}

		String result = columns.stream().collect(Collectors.joining(", "));
		return String.format("CREATE TABLE %s (%s)", MDB2SQLite.escape(table.getName()), result);
	}

	/**
	 * Create an index in an SQLite table for the corresponding index in MS Access
	 * 
	 * @param table MS Access table
	 * @throws SQLException
	 */
	@SuppressWarnings("unchecked")
	private static String createIndexStatement(Index index) throws SQLException {
		List<String> columns = new ArrayList<String>();
		for (ColumnDescriptor column : (List<ColumnDescriptor>) index.getColumns()) {
			columns.add(MDB2SQLite.escape(column.getName()));
		}
		String unique = index.isUnique() ? "UNIQUE" : "";
		String tableName = index.getTable().getName();
		String indexName = tableName + "_" + index.getName();
		String result = columns.stream().collect(Collectors.joining(", "));
		return String.format("CREATE %s INDEX %s ON %s(%s)", unique, MDB2SQLite.escape(indexName),
				MDB2SQLite.escape(tableName), result);
	}

	/**
	 * Populate the SQLite table
	 *
	 * @param sqlite
	 * @param mdbTable
	 * @throws SQLException
	 * @throws SqlJetException
	 */
	@SuppressWarnings("unchecked")
	private static void populateTable(SqlJetDb sqlite, Table mdbTable) throws SQLException, SqlJetException {
		List<Column> columns = (List<Column>) mdbTable.getColumns();
		sqlite.beginTransaction(SqlJetTransactionMode.WRITE);
		try {
			ISqlJetTable table = sqlite.getTable(mdbTable.getName());

			// Bind all the column values
			for (Map<String, Object> row : mdbTable) {
				List<Object> values = new ArrayList<Object>();
				for (Column column : columns) {
					Object value = row.get(column.getName());

					// If null, just bail out early and avoid a lot of NULL checking
					if (value == null) {
						values.add(value);
						continue;
					}

					// Perform any conversions
					switch (column.getType()) {
						case MONEY: // Store money as a string.
							values.add(value.toString());
							break;

						case BOOLEAN: // SQLite has no booleans
							values.add(((Boolean) value).booleanValue() ? "1" : 0);
							break;

						default:
							values.add(value);
							break;
					}
				}
				table.insert(values.toArray());
			}
		} finally {
			sqlite.commit();
		}
	}

	/**
	 * Manual escaping of identifiers.
	 * 
	 * @param identifier
	 * @return
	 */
	private static final String escape(String identifier) {
		return "'" + identifier.replace("'", "''") + "'";
	}
}
