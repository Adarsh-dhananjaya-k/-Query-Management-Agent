from table_db import get_all_tickets_df

def inspect_data():
    df = get_all_tickets_df()
    print("Unique Teams in 'Assigned Team' column:")
    print(df["Assigned Team"].unique())
    print("\nSample Data (first 5 rows):")
    print(df[["Ticket ID", "Assigned Team", "User Name"]].head())

if __name__ == "__main__":
    inspect_data()
