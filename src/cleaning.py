def clean_title_basics(df):
    return (
        df.dropDuplicates(["tconst"])
          .drop("endYear")
          .fillna({"genres": "Unknown"})
    )

def clean_title_ratings(df):
    return df.dropDuplicates(["tconst"])
