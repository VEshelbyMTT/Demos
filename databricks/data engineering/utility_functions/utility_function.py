
def save_as_delta(df, folder_name, base_path):
    try:
        folder_path = f"{base_path}/{folder_name}"
        delta_path = f"{folder_path}/{folder_name}_cleaned"

        # Save as Delta
        df.write.format("delta").mode("overwrite").save(delta_path)

        return delta_path
        
    except Exception as e:
        print(f"Error saving data: {e}")


def save_or_update_deltatable(df, folder_name, catalog="azure_cloud", schema="default"):
    table_name = f"{catalog}.{schema}.{folder_name}_cleaned"
    df.write.format("delta").mode("overwrite").saveAsTable(table_name)
    return table_name