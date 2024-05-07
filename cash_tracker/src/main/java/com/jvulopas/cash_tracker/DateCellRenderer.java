package com.jvulopas.cash_tracker;

import java.awt.Component;
import java.text.SimpleDateFormat;
import java.util.Date;

import javax.swing.JLabel;
import javax.swing.JTable;
import javax.swing.table.TableCellRenderer;

@SuppressWarnings("serial")
public class DateCellRenderer extends JLabel implements TableCellRenderer {
	
	private static SimpleDateFormat sdf = new SimpleDateFormat("MMM dd");
	public Component getTableCellRendererComponent(JTable table, Object value, boolean isSelected, boolean hasFocus,
			int row, int column) {
		
		String outp = "";
		try {
			outp = sdf.format((Date) value);
		} catch (Exception e) {
			outp = value.toString();
		}
		setText(outp);
		
		// TODO setToolTipText
		return this;
	}

}
