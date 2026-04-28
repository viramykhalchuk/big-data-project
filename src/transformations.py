import io
import shutil
from contextlib import redirect_stdout
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.window import Window


def save_result_as_single_csv(result_df, output_dir, file_name):
    output_dir = Path(output_dir)
    temp_dir = output_dir / f"{file_name}_temp"
    final_file = output_dir / f"{file_name}.csv"

    if temp_dir.exists():
        shutil.rmtree(temp_dir)

    if final_file.exists():
        final_file.unlink()

    result_df.coalesce(1).write.mode("overwrite").option("header", True).csv(str(temp_dir))

    part_files = list(temp_dir.glob("part-*.csv"))
    if part_files:
        part_files[0].rename(final_file)

    shutil.rmtree(temp_dir)


def save_execution_plan(result_df, output_dir, file_name):
    output_dir = Path(output_dir)
    plan_file = output_dir / f"{file_name}_execution_plan.txt"

    buffer = io.StringIO()
    with redirect_stdout(buffer):
        result_df.explain(mode="formatted")

    plan_file.write_text(buffer.getvalue(), encoding="utf-8")


def print_and_save_question_result(number, question, result_df, output_dir, rows=10):
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    file_prefix = f"question_{number:02d}"

    print("\n" + "=" * 120)
    print(f"BUSINESS QUESTION {number}")
    print(question)
    print("=" * 120)

    result_df.show(rows, truncate=False)

    print(f"\nEXECUTION PLAN FOR BUSINESS QUESTION {number}")
    result_df.explain(mode="formatted")

    save_result_as_single_csv(result_df, output_dir, f"{file_prefix}_result")
    save_execution_plan(result_df, output_dir, file_prefix)

    print(f"\nSaved result: {output_dir / f'{file_prefix}_result.csv'}")
    print(f"Saved execution plan: {output_dir / f'{file_prefix}_execution_plan.txt'}")


def get_movies_with_ratings(title_basics_df, title_ratings_df, min_votes):
    return (
        title_basics_df.alias("b")
        .join(title_ratings_df.alias("r"), "tconst", "inner")
        .filter(
            (F.col("titleType") == "movie") &
            (F.col("isAdult") == 0) &
            F.col("startYear").isNotNull() &
            F.col("averageRating").isNotNull() &
            F.col("numVotes").isNotNull() &
            (F.col("numVotes") >= min_votes)
        )
        .select(
            "tconst",
            "primaryTitle",
            "originalTitle",
            "startYear",
            "runtimeMinutes",
            "genres",
            "averageRating",
            "numVotes"
        )
    )


def question_1_top_movies_after_2000(title_basics_df, title_ratings_df):
    return (
        get_movies_with_ratings(title_basics_df, title_ratings_df, 100000)
        .filter(F.col("startYear") >= 2000)
        .orderBy(F.desc("averageRating"), F.desc("numVotes"))
        .limit(10)
    )


def question_2_best_genres(title_basics_df, title_ratings_df):
    movies = get_movies_with_ratings(title_basics_df, title_ratings_df, 10000)

    return (
        movies
        .withColumn("genre", F.explode(F.split(F.col("genres"), ",")))
        .filter(F.col("genre") != "Unknown")
        .groupBy("genre")
        .agg(
            F.count("*").alias("movie_count"),
            F.round(F.avg("averageRating"), 3).alias("avg_rating"),
            F.sum("numVotes").alias("total_votes"),
            F.round(F.avg("runtimeMinutes"), 2).alias("avg_runtime")
        )
        .filter(F.col("movie_count") >= 1000)
        .orderBy(F.desc("avg_rating"), F.desc("total_votes"))
        .limit(10)
    )


def question_3_best_movie_by_decade(title_basics_df, title_ratings_df):
    movies = (
        get_movies_with_ratings(title_basics_df, title_ratings_df, 50000)
        .filter((F.col("startYear") >= 1950) & (F.col("startYear") <= 2026))
        .withColumn("decade_start", (F.floor(F.col("startYear") / 10) * 10).cast("int"))
    )

    window_spec = Window.partitionBy("decade_start").orderBy(
        F.desc("averageRating"),
        F.desc("numVotes")
    )

    return (
        movies
        .withColumn("rank_in_decade", F.row_number().over(window_spec))
        .filter(F.col("rank_in_decade") == 1)
        .withColumn("decade", F.concat(F.col("decade_start"), F.lit("s")))
        .select(
            "decade",
            "primaryTitle",
            "startYear",
            "genres",
            "averageRating",
            "numVotes",
            "rank_in_decade"
        )
        .orderBy("decade")
    )


def question_4_top_movies_by_genre_after_2010(title_basics_df, title_ratings_df):
    movies = (
        get_movies_with_ratings(title_basics_df, title_ratings_df, 25000)
        .filter(F.col("startYear") >= 2010)
        .withColumn("genre", F.explode(F.split(F.col("genres"), ",")))
        .filter(F.col("genre") != "Unknown")
    )

    window_spec = Window.partitionBy("genre").orderBy(
        F.desc("averageRating"),
        F.desc("numVotes")
    )

    return (
        movies
        .withColumn("rank_in_genre", F.row_number().over(window_spec))
        .filter(F.col("rank_in_genre") <= 3)
        .select(
            "genre",
            "rank_in_genre",
            "primaryTitle",
            "startYear",
            "averageRating",
            "numVotes"
        )
        .orderBy("genre", "rank_in_genre")
    )


def question_5_title_type_summary(title_basics_df, title_ratings_df):
    return (
        title_basics_df.alias("b")
        .join(title_ratings_df.alias("r"), "tconst", "inner")
        .filter(
            (F.col("isAdult") == 0) &
            (F.col("startYear") >= 2000) &
            F.col("averageRating").isNotNull() &
            F.col("numVotes").isNotNull() &
            (F.col("numVotes") >= 1000)
        )
        .groupBy("titleType")
        .agg(
            F.count("*").alias("title_count"),
            F.round(F.avg("averageRating"), 3).alias("avg_rating"),
            F.round(F.avg("numVotes"), 2).alias("avg_votes"),
            F.round(F.avg("runtimeMinutes"), 2).alias("avg_runtime")
        )
        .filter(F.col("title_count") >= 1000)
        .orderBy(F.desc("title_count"))
    )


def question_6_ukrainian_localized_movies(title_basics_df, title_ratings_df, title_akas_df):
    ukrainian_titles = (
        title_akas_df
        .filter((F.col("region") == "UA") | (F.lower(F.col("language")) == "uk"))
        .groupBy(F.col("titleId").alias("tconst"))
        .agg(
            F.first("title", ignorenulls=True).alias("localizedTitle"),
            F.first("region", ignorenulls=True).alias("region"),
            F.first("language", ignorenulls=True).alias("language")
        )
    )

    return (
        title_basics_df.alias("b")
        .join(ukrainian_titles.alias("a"), "tconst", "inner")
        .join(title_ratings_df.alias("r"), "tconst", "inner")
        .filter(
            (F.col("titleType") == "movie") &
            (F.col("isAdult") == 0) &
            F.col("averageRating").isNotNull() &
            F.col("numVotes").isNotNull() &
            (F.col("numVotes") >= 1000)
        )
        .select(
            "primaryTitle",
            "localizedTitle",
            "startYear",
            "genres",
            "region",
            "language",
            "averageRating",
            "numVotes"
        )
        .orderBy(F.desc("averageRating"), F.desc("numVotes"))
        .limit(10)
    )


def run_business_questions(title_basics_df, title_ratings_df, title_akas_df, output_dir):
    q1 = question_1_top_movies_after_2000(title_basics_df, title_ratings_df)
    print_and_save_question_result(
        1,
        "What are the top 10 highest-rated non-adult movies released after 2000 with at least 100000 votes?",
        q1,
        output_dir,
        10
    )

    q2 = question_2_best_genres(title_basics_df, title_ratings_df)
    print_and_save_question_result(
        2,
        "Which movie genres have the highest average rating among movies with at least 10000 votes?",
        q2,
        output_dir,
        10
    )

    q3 = question_3_best_movie_by_decade(title_basics_df, title_ratings_df)
    print_and_save_question_result(
        3,
        "What is the best-rated movie in each decade from 1950 to 2020s?",
        q3,
        output_dir,
        20
    )

    q4 = question_4_top_movies_by_genre_after_2010(title_basics_df, title_ratings_df)
    print_and_save_question_result(
        4,
        "What are the top 3 highest-rated movies in each genre after 2010?",
        q4,
        output_dir,
        50
    )

    q5 = question_5_title_type_summary(title_basics_df, title_ratings_df)
    print_and_save_question_result(
        5,
        "How do different title types compare by count, average rating, average votes and average runtime after 2000?",
        q5,
        output_dir,
        20
    )

    q6 = question_6_ukrainian_localized_movies(title_basics_df, title_ratings_df, title_akas_df)
    print_and_save_question_result(
        6,
        "What are the top-rated movies that have Ukrainian region or Ukrainian language localization?",
        q6,
        output_dir,
        10
    )