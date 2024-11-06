from Utils.database_utils import read_table_from_db, prod_db_type


def get_data_id_set(df):
    # Extract 'data_id' column and store it in a set for efficient lookup
    data_id_set = set(df["data_id"])
    return data_id_set


# Example usage
df = read_table_from_db("bronze_nexen_cash_and_security_transactions", prod_db_type)
data_id_set = get_data_id_set(df)


import pickle


def save_data_id_to_file(data_id_set, filepath):
    with open(filepath, "wb") as file:
        pickle.dump(data_id_set, file)
    print(f"'data_id' set has been saved to {filepath}")


# Example usage to save the set
save_data_id_to_file(data_id_set, "Reporting/Bronze_tables/data_id_list.pkl")


# Function to load the set back
def load_data_id_from_file(filepath):
    with open(filepath, "rb") as file:
        return pickle.load(file)


# Example usage to load the set
loaded_data_id_set = load_data_id_from_file("Reporting/Bronze_tables/data_id_list.pkl")
