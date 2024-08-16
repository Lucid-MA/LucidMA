import numpy as np
import pandas as pd

# Load the original data
file_path = r"C:\Users\Tony.Hoang\OneDrive - Lucid Management and Capital Partne\Desktop\Capital Account Balance CSV.csv"  # Replace with your actual file path
data = pd.read_csv(file_path)

# Define a pool of sensible names for each category
investor_names = [
    "Orion Ventures",
    "Lunar Capital",
    "Solaris Holdings",
    "Pioneer Fund",
    "Astra Investment",
    "Nova Partners",
    "Zenith Global",
    "Atlas Group",
    "Quantum Capital",
    "Horizon Trust",
    "Vertex Investments",
    "Aurora Capital",
    "Stellar Holdings",
    "Galaxy Partners",
    "Polaris Ventures",
    "Cosmos Fund",
    "Stratosphere Capital",
    "Meteor Investments",
    "Nebula Ventures",
    "Comet Partners",
    "Radiant Investments",
    "Celestial Capital",
    "Eclipse Holdings",
    "Vortex Capital",
    "Pinnacle Fund",
    "Summit Investments",
    "Infinity Partners",
    "Equinox Capital",
    "Odyssey Fund",
    "Vertex Trust",
    "Interstellar Capital",
    "Solar Flare Ventures",
    "Aurora Holdings",
    "Nebula Trust",
    "Quantum Ventures",
    "Zenith Holdings",
    "Nova Fund",
    "Atlas Investments",
    "Pioneer Ventures",
    "Astra Holdings",
    "Orion Fund",
    "Lunar Trust",
    "Stellar Capital",
    "Galaxy Holdings",
    "Polaris Capital",
    "Stratosphere Ventures",
    "Meteor Fund",
    "Comet Holdings",
    "Radiant Ventures",
    "Celestial Trust",
    "Eclipse Capital",
    "Vortex Holdings",
    "Summit Ventures",
    "Infinity Trust",
    "Odyssey Capital",
    "Interstellar Ventures",
    "Solar Flare Fund",
    "Aurora Trust",
    "Nebula Capital",
    "Zenith Ventures",
    "Nova Holdings",
    "Atlas Fund",
    "Pioneer Trust",
    "Astra Ventures",
    "Orion Holdings",
    "Lunar Fund",
    "Solaris Ventures",
    "Horizon Capital",
    "Vertex Ventures",
    "Aurora Fund",
    "Galaxy Ventures",
    "Polaris Holdings",
    "Stellar Fund",
    "Comet Capital",
    "Pinnacle Trust",
    "Eclipse Ventures",
    "Vortex Fund",
    "Pioneer Capital",
    "Zenith Trust",
    "Infinity Capital",
    "Quantum Holdings",
    "Odyssey Ventures",
    "Summit Capital",
    "Cosmos Ventures",
    "Stratosphere Fund",
    "Radiant Capital",
    "Interstellar Trust",
]

group_names = [f"Group {i+1}" for i in range(32)]
type_names = [
    "Equity Fund",
    "Hedge Fund",
    "Pension Fund",
    "Venture Capital",
    "Real Estate Fund",
    "Private Equity",
    "Endowment",
    "Foundation",
    "Mutual Fund",
    "Sovereign Wealth Fund",
]
fund_names = ["Alpha Fund", "Beta Fund", "Gamma Fund"]
series_names = [
    f"Series {chr(65+i)}" for i in range(11)
]  # Generates Series A to Series K

# Create mappings for each column
investor_mapping = dict(zip(data["Investor"].unique(), investor_names))
group_mapping = dict(zip(data["Group"].unique(), group_names))
type_mapping = dict(zip(data["Type"].unique(), type_names))
fund_mapping = dict(zip(data["Fund"].unique(), fund_names))
series_mapping = dict(zip(data["Series"].unique(), series_names))

# Apply the mappings to the data
data["Investor"] = data["Investor"].map(investor_mapping)
data["Group"] = data["Group"].map(group_mapping)
data["Type"] = data["Type"].map(type_mapping)
data["Fund"] = data["Fund"].map(fund_mapping)
data["Series"] = data["Series"].map(series_mapping)

# Convert financial columns to numeric, replacing errors with 0
financial_columns = ["Pre AUM", "Subscriptions", "Redemptions", "Post AUM"]
for col in financial_columns:
    data[col] = pd.to_numeric(
        data[col].str.replace(",", "").str.replace("-", "0"), errors="coerce"
    ).fillna(0)

# Recalculate Post AUM
data["Post AUM"] = data["Pre AUM"] + data["Subscriptions"] - data["Redemptions"]

# Divide all financial columns by 55 and round up
data[financial_columns] = np.ceil(data[financial_columns] / 55)

# Save the masked data to a new CSV file
output_file_path = r"C:\Users\Tony.Hoang\OneDrive - Lucid Management and Capital Partne\Desktop\masked_capital_account_balance.csv"  # Replace with your desired output path
data.to_csv(output_file_path, index=False)

print(f"Masked data saved to {output_file_path}")
