package com.jvulopas.cash_tracker;

import java.awt.Dimension;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.table.DefaultTableModel;

public class QueryResultTable extends JPanel {

	private DefaultTableModel tableModel;
	private JTable table;
	
	public QueryResultTable() {
		super();
		
		JLabel noData = new JLabel("No queries run yet.");
		this.add(noData);
//		this.table = new JTable();
//		table.setVisible(true);
//		this.add(new JScrollPane(table));
	}
	
	
	public void populate(ResultSet helixData) throws SQLException {
		this.removeAll();
		tableModel = tableModelOfResultSet(helixData);
		table = new JTable(tableModel);
		table.setAutoResizeMode(JTable.AUTO_RESIZE_OFF);
		TableColumnAdjuster tca = new TableColumnAdjuster(table);
		tca.setColumnHeaderIncluded(true);
		tca.adjustColumns();
		table.setPreferredScrollableViewportSize(
			    new Dimension(
			        table.getPreferredSize().width,
			        table.getRowHeight() * Math.max(table.getRowCount(), 15)));
		JScrollPane scrollPane = new JScrollPane(table);
		this.add(scrollPane);
	}
	
	public static DefaultTableModel tableModelOfResultSet(ResultSet rs) throws SQLException {
		
		ResultSetMetaData metaData = rs.getMetaData();

		// names of columns
	    Vector<String> colNames = new Vector<String>();
	    int numCols = metaData.getColumnCount();
	    for (int i = 1; i <= numCols; i++) {
	    	colNames.add(metaData.getColumnName(i));
	    }

	    // data of the table
	    Vector<Vector<Object>> data = new Vector<Vector<Object>>();
	    
	    while (rs.next()) {
	        Vector<Object> vector = new Vector<Object>();
	        for (int i = 1; i <= numCols; i++) {
	            vector.add(rs.getObject(i));
	        }
	        data.add(vector);
	    }

	    return new DefaultTableModel(data, colNames);
	}
	
}
