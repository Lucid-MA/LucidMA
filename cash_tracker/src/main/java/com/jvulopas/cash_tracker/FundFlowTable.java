package com.jvulopas.cash_tracker;

import java.awt.Color;
import java.awt.Component;
import java.awt.Dimension;
import java.awt.Font;
import java.awt.GridLayout;
import java.awt.Point;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Optional;

import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.table.TableCellRenderer;
import javax.swing.table.TableColumnModel;

@SuppressWarnings("serial")
public class FundFlowTable extends JPanel {
	
	private FundFlowTableModel tableModel;
	private JTable table;
	private static final SimpleDateFormat SDF = new SimpleDateFormat("MM/dd/yyyy");
	
	private int chosenRow = -1;
	private int chosenCol = -1;
	private Flow chosenFlow;
	//private String zoomLabelInfo;
	
	
	public FundFlowTable() {
		super();
		table = new JTable();
		table.setAutoResizeMode(JTable.AUTO_RESIZE_ALL_COLUMNS);
		table.setVisible(true);
		
		this.add(new JScrollPane(table));
		this.setLayout(new GridLayout(1,1));
		chosenFlow = null;
		
	}
	
	public void declareSettle(boolean afterSweep) {
		// TODO settle through the bnym account so will also settle all allocations
		if (chosenFlow == null) {
			return;
		}
		if (chosenFlow.hasSettled().isPresent()) {
			if (chosenFlow.hasSettled().get()) {
				JOptionPane.showMessageDialog(null, "Flow has already settled.", "Already settled.", JOptionPane.INFORMATION_MESSAGE);
				return;
			}
		}
		
		chosenFlow.declareSettled(afterSweep);
		System.out.println(chosenFlow.getTransaction().getActionID() + " declared settled manually.");
	}
	
	private void setTableAndModel(FundFlowTableModel model) {
		this.removeAll();
		tableModel = model;
		table = new JTable(tableModel);
		TableColumnModel colModel = table.getColumnModel();
		colModel.getColumn(0).setCellRenderer(new DateCellRenderer());
		
		final TableCellRenderer acr = new AmountCellRenderer();
		for (int setCol = 1; setCol < colModel.getColumnCount() - 1; setCol++) {
			colModel.getColumn(setCol).setCellRenderer(acr);
		}

		table.addMouseListener(new MouseAdapter() {
			@Override
			public void mouseClicked(MouseEvent e) {
				Point xy = e.getPoint();
				JTable tbl = (JTable) e.getSource();
				int rowNum = (tbl).rowAtPoint(xy);
				int colNum = (tbl).columnAtPoint(xy);
				
				chosenRow = rowNum;
				chosenCol = colNum;
				//zoomLabelInfo = rowNum + ",,,,";
				//tbl.setToolTipText(tbl.getModel().getValueAt(chosenRow, chosenCol).toString());
				tbl.repaint();
				//((JLabel) tbl.getCellRenderer(rowNum, colNum)).setToolTipText(rowNum + ", " + colNum);
			}
			
			
		});
		
		
		
		table.setAutoResizeMode(JTable.AUTO_RESIZE_OFF);		
		TableColumnAdjuster tca = new TableColumnAdjuster(table);
		tca.setColumnHeaderIncluded(true);
		tca.adjustColumns();
		table.setPreferredScrollableViewportSize(
			    new Dimension(
			        table.getPreferredSize().width,
			        table.getRowHeight() * Math.max(table.getRowCount(), 15)));
		JScrollPane scrollPane = new JScrollPane(table);
		//scrollPane.setPreferredSize(table.getPreferredScrollableViewportSize());
		
		
		this.add(scrollPane);
	}
	
	public Fund getCurrFund() {
		return tableModel.getCurrFund();
	}
	
	public String getZoomLabelInfo() {
		//return this.zoomLabelInfo;
		return "for testing... todo";
		//Flow fl = tableModel.getFlowBehindCell(chosenRow, chosenCol);
		//return fl.getSecurity();
	}
	/**
	 * Change fund. Display (default) available cash balance
	 * @param fund
	 * @param valDate
	 */
	public void setFund(Fund fund, Date valDate) {
		setTableAndModel(new FundFlowTableModel(fund, valDate));
	}
	
	/**
	 * Change all info to display, ie, set the table
	 * @param fund
	 * @param valDate
	 * @param security
	 * @param status
	 * @param seriesChoice 
	 */
	public void set(Fund fund, Date valDate, String security, HoldingStatus status, String seriesChoice) { 
		setTableAndModel(new FundFlowTableModel(fund, valDate, security, status, seriesChoice));
	}
	
	/**
	 * Keeps same fund
	 * @param security
	 * @param status
	 */
	public void setSecurity(String security, HoldingStatus status) {
		setTableAndModel(new FundFlowTableModel(tableModel.getCurrFund(), tableModel.getCurrDate(), security, status, tableModel.getCurrSeries()));
	}
	
	public void reload() {
		setTableAndModel(new FundFlowTableModel(tableModel.getCurrFund(), tableModel.getCurrDate(), tableModel.getCurrSecurity(), tableModel.getCurrHoldingStatus(),tableModel.getCurrSeries()));
	}

	public void setSeries(String seriesChoice) {
		setTableAndModel(new FundFlowTableModel(tableModel.getCurrFund(), tableModel.getCurrDate(), tableModel.getCurrSecurity(), tableModel.getCurrHoldingStatus(), seriesChoice));
		
	}
	
	
	@SuppressWarnings("serial")
	private class AmountCellRenderer extends JLabel implements TableCellRenderer {
		
		//private static final DecimalFormat centsPlace = new DecimalFormat("0.00");
		public Component getTableCellRendererComponent(JTable table, Object value, boolean isSelected, boolean hasFocus,
				int row, int column) {
			
			Font font = getFont();
			String outp = "";
			setForeground(Color.black);
			setOpaque(true);
			double val = 0;
			if (column % 2 != 0) {
				try {
					val = ((Flow) value).getAmount();
				} catch (Exception e){
					// TODO should never enter
				}
			} else {
				val = Double.parseDouble(value + "");
			}
			try {
				outp = String.format("%,.2f", Math.abs(val));
				
				if (val < 0) {
					outp = "(" + outp + ")";
					//setForeground(Color.red);
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
				// balance col
				setBackground(Color.LIGHT_GRAY);
				
				setFont(font.deriveFont(font.getStyle() | Font.BOLD));
			} else {
				// here if flow
				String toolTipText = "";
				setBackground(table.getBackground());
				try {
					Flow fl = (Flow) value;

					if (isSelected) {
						System.out.println("You've selected " + fl.getTransaction().getActionID());
						chosenFlow = fl;
					}
					
					Optional<Boolean> settledness = fl.hasSettled();
					if (settledness.isPresent()) {
						if (!settledness.get()) {
							// if failing
							setBackground(Color.red);
							toolTipText = "<html><b>FAILING<b> from " + SDF.format(fl.getTransaction().getDate().get()) + "<br>";
						} else {
							// if settled
							setBackground(Color.green);
							toolTipText = "<html>SETTLED <br>";
						}
					} else {
						toolTipText = "<html>NOT YET SETTLED <br>";
					}
					
					String addlAllocs = "";
					double checkSum = 0;
					for (Flow allocFl : tableModel.getCurrFund().getAllocations(fl)) {
						addlAllocs = addlAllocs +  String.format("%,.2f", allocFl.getAmount()) + "<br>";
						checkSum += allocFl.getAmount();
					}
					if (Math.abs(checkSum - fl.getAmount()) > 0.02) {
						toolTipText = toolTipText + "<b>NOT ALLOCATED</b>";
					} else {
						toolTipText = toolTipText + addlAllocs;
					}
					setToolTipText(toolTipText + "</html>");
				} catch (Exception e){
					
				}
				setFont(font.deriveFont(font.getStyle() & ~Font.BOLD));
			}
			
			if (chosenCol == column && chosenRow == row) {
                setBorder(BorderFactory.createLineBorder(Color.BLUE, 2));
                
            }
            else {
                setBorder(BorderFactory.createEmptyBorder(2, 2, 2, 2));
            }
			
			if (isSelected) {
				System.out.println("Selected " + row + ", " + column);
			}
			
			return this;
		}

	}
	
}
