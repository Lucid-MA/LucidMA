package com.jvulopas.cash_tracker;

import java.awt.Color;
import java.awt.Component;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import javax.swing.JLabel;
import javax.swing.JTable;
import javax.swing.table.TableCellRenderer;

import java.awt.Font;

@SuppressWarnings("serial")
public class OLDAmountCellRenderer extends JLabel implements TableCellRenderer {
	
	//private static final DecimalFormat centsPlace = new DecimalFormat("0.00"); git 28
	public Component getTableCellRendererComponent(JTable table, Object value, boolean isSelected, boolean hasFocus,
			int row, int column) {
		
		Font font = getFont();
		String outp = "";
		setForeground(Color.black);
		try {
			double val = Double.parseDouble(value + "");
			outp = String.format("%,.2f", Math.abs(val));
			
			if (val < 0) {
				outp = "(" + outp + ")";
				setForeground(Color.red);
			}
			
			if (val == 0) {
				outp = "-";
			}
		//	outp = centsPlace.format(Double.parseDouble(value + ""));
		} catch (Exception e) {
			outp = value.toString();
		}
		setText(outp);
		
		if (column % 2 == 0) {
			System.out.println(column);
			setFont(font.deriveFont(font.getStyle() | Font.BOLD));
		} else {
			setFont(font.deriveFont(font.getStyle() & ~Font.BOLD));
		}
		
//		if (isSelected) {
//			System.out.println("Selected " + row + ", " + column);
//		}
//		
//		if (hasFocus) {
//			System.out.println("Focus " + row + ", " + column);
//		}
		
		// TODO setToolTipText
		return this;
	}

}
