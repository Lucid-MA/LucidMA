package com.jvulopas.cash_tracker;

import java.awt.Dimension;
import java.awt.GridLayout;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Vector;

import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.table.DefaultTableModel;
import javax.swing.table.TableCellRenderer;
import javax.swing.table.TableColumnModel;

@SuppressWarnings("serial")
public class PlainTable extends JPanel {
	private DefaultTableModel model;
	private JTable table;
	
	public PlainTable() {
		super();
		JLabel noData = new JLabel("No data to show yet.");
		this.add(noData);
	}
	
	public void populate(Vector<Vector<Object>> data, Vector<String> colNames) {
		this.removeAll();
		model = new DefaultTableModel(data, colNames);
		table = new JTable(model);
		table.setAutoResizeMode(JTable.AUTO_RESIZE_OFF);
		table.setVisible(true);
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
	
}